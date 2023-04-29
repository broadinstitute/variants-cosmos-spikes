package org.broadinstitute.gvs.azure.cosmos;

import ch.qos.logback.classic.Level;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CosmosIngest {

    private static final Logger logger = LoggerFactory.getLogger(CosmosIngest.class);

    public static void main(String[] argv) {
        configureLogging();
        CosmosEndpointAndKey endpointAndKey = CosmosEndpointAndKey.fromEnvironment();
        IngestArguments ingestArguments = IngestArguments.parseArgs(argv);
        List<Path> avroPaths = AvroReader.findAvroPaths(ingestArguments.getAvroDir());

        try (CosmosAsyncClient client = buildClient(endpointAndKey)) {
            CosmosAsyncContainer container = client.
                    getDatabase(ingestArguments.getDatabase()).
                    getContainer(ingestArguments.getContainer());

            loadAvros(container, avroPaths, ingestArguments);
        }
    }

    public static void loadAvros(CosmosAsyncContainer container, Iterable<Path> avroPaths, IngestArguments ingestArguments) {
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicLong recordCounter = new AtomicLong();
        AtomicLong documentCounter = new AtomicLong();
        AtomicLong submissionBatchCounter = new AtomicLong();

        // Continuous Flux loading is faster if the container throughput is sufficiently high (~10K RU/s in my limited
        // experience), but will quickly crash out this loader with non-retryable 429s if throughput is too low.
        if (ingestArguments.isContinuousFlux()) {
            Flux<CosmosBulkItemResponse> responseFlux = Flux.fromIterable(avroPaths).flatMap(
                    avroPath -> {
                        Flux<CosmosItemOperation> itemFlux =
                                AvroReader.itemFluxFromAvroPath(objectMapper, avroPath, ingestArguments, recordCounter, documentCounter);
                        return itemFlux.buffer(ingestArguments.getSubmissionBatchSize()).flatMap(batch -> {
                            Flux<CosmosItemOperation> submissionBatchFlux = Flux.fromIterable(batch);
                            logger.info("Submitting batch " + submissionBatchCounter.incrementAndGet() + " at counter " + documentCounter.get());
                            return executeItemOperationsWithErrorHandling(container, submissionBatchFlux);
                        });
                    }
            );
            responseFlux.blockLast();
        } else {
            for (Path avroPath : avroPaths) {
                logger.info(String.format("Processing Avro file '%s'...", avroPath));
                Flux<CosmosItemOperation> itemFlux =
                        AvroReader.itemFluxFromAvroPath(objectMapper, avroPath, ingestArguments, recordCounter, documentCounter);
                executeItemOperationsWithErrorHandling(container, itemFlux).blockLast();
                logger.info(String.format("Avro file '%s' processing complete.", avroPath));
            }
        }
    }

    private static Flux<CosmosBulkItemResponse> executeItemOperationsWithErrorHandling(CosmosAsyncContainer container,
                                                                                       Flux<CosmosItemOperation> itemOperations) {
        // Only the first and last few lines are the "execute" bits, all the rest is error handling iff something goes wrong.
        return container.executeBulkOperations(itemOperations).flatMap(operationResponse -> {
            CosmosBulkItemResponse itemResponse = operationResponse.getResponse();
            CosmosItemOperation itemOperation = operationResponse.getOperation();

            if (operationResponse.getException() != null) {
                logger.error("Bulk operation failed: " + operationResponse.getException());
            } else if (itemResponse == null || !itemResponse.isSuccessStatusCode()) {
                ObjectNode objectNode = itemOperation.getItem();
                logger.error(String.format(
                        "The operation for Item ID: [%s]  Item PartitionKey Value: [%s] did not complete " +
                                "successfully with a %s/%s response code.",
                        objectNode.get("id"),
                        objectNode.get("sample_id"),
                        itemResponse != null ? itemResponse.getStatusCode() : "n/a",
                        itemResponse != null ? itemResponse.getSubStatusCode() : "n/a"));
            }

            if (itemResponse == null) {
                return Mono.error(new IllegalStateException("No response retrieved."));
            } else {
                return Mono.just(itemResponse);
            }
        });
    }

    private static void configureLogging() {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
    }

    private static CosmosAsyncClient buildClient(CosmosEndpointAndKey endpointAndKey) {
        return new CosmosClientBuilder().
                endpoint(endpointAndKey.getEndpoint()).
                key(endpointAndKey.getKey()).
                preferredRegions(List.of("East US")).
                contentResponseOnWriteEnabled(true).
                consistencyLevel(ConsistencyLevel.SESSION).
                buildAsyncClient();
    }
}
