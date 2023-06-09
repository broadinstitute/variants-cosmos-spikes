package org.broadinstitute.gvs.azure.cosmos;

import ch.qos.logback.classic.Level;
import com.azure.cosmos.*;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
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

            if (ingestArguments.getTargetThroughput() != null) {
                ThroughputControlGroupConfig groupConfig =
                        new ThroughputControlGroupConfigBuilder()
                                .groupName("local-throughput-group")
                                .targetThroughput(ingestArguments.getTargetThroughput())
                                .build();
                container.enableLocalThroughputControlGroup(groupConfig);
            }

            loadAvroFiles(container, avroPaths, ingestArguments);
        }
    }

    public static void loadAvroFiles(CosmosAsyncContainer container, Iterable<Path> avroPaths, IngestArguments ingestArguments) {
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicLong recordCounter = new AtomicLong();
        AtomicLong documentCounter = new AtomicLong();
        AtomicLong submissionBatchCounter = new AtomicLong();
        int submissionBatchSize = ingestArguments.getSubmissionBatchSize();

        CosmosBulkExecutionOptions bulkExecutionOptions = buildCosmosBulkExecutionOptions(ingestArguments);

        // Continuous Flux loading is faster if the container throughput is sufficiently high (>= ~10K RU/s in my
        // limited experience), but will quickly crash this loader with non-retryable 429s if throughput is too low.
        // At the time of this writing, continuous flux is not a good choice for serverless Cosmos since serverless
        // Cosmos has fixed 5K RU/s throughput.
        if (ingestArguments.isContinuousFlux()) {
            Flux<CosmosBulkItemResponse> responseFlux = Flux.fromIterable(avroPaths).flatMap(
                    avroPath -> {
                        Flux<CosmosItemOperation> itemFlux =
                                AvroReader.itemFluxFromAvroPath(objectMapper, avroPath, ingestArguments, recordCounter, documentCounter);

                        // This strange-looking buffering / non-overlapping sliding window construct worked around
                        // stallouts in the client library. I'm not sure why this was necessary (I would have thought
                        // the client library would work this out itself) but without it the client library stopped
                        // sending data to Cosmos and items would just back up in memory until the loader crashed due to
                        // memory exhaustion.
                        return itemFlux.buffer(submissionBatchSize).flatMap(
                                batch -> {
                                    logger.info("Submitting batch " + submissionBatchCounter.incrementAndGet() + " at document counter " + documentCounter.get());
                                    return executeItemOperationsWithErrorHandling(container, Flux.fromIterable(batch), bulkExecutionOptions);
                                });
                    }
            );
            responseFlux.blockLast();
        } else {
            // Slow but steady non-continuous Flux. This has the disadvantage of not overlapping the Avro processing
            // with the sending of data to Cosmos. Non-continuous Flux ties up the VM for longer than necessary and
            // lengthens the time to load data, but currently enjoys the advantage of not crashing with low container
            // throughput like what is available on serverless Cosmos.
            for (Path avroPath : avroPaths) {
                logger.info(String.format("Processing Avro file '%s'...", avroPath));

                Flux<CosmosItemOperation> itemFlux =
                        AvroReader.itemFluxFromAvroPath(objectMapper, avroPath, ingestArguments, recordCounter, documentCounter);

                for (List<CosmosItemOperation> submissionBatch : itemFlux.buffer(submissionBatchSize).toIterable()) {
                    executeItemOperationsWithErrorHandling(container, Flux.fromIterable(submissionBatch), bulkExecutionOptions).blockLast();
                }

                logger.info(String.format("Avro file '%s' processing complete.", avroPath));
            }
        }
    }

    private static CosmosBulkExecutionOptions buildCosmosBulkExecutionOptions(IngestArguments ingestArguments) {
        // No idea what this bridge stuff is about, most of the getters/setters are not public on CosmosBulkExecutionOptions.
        ImplementationBridgeHelpers.CosmosBulkExecutionOptionsHelper.CosmosBulkExecutionOptionsAccessor accessor =
                ImplementationBridgeHelpers.CosmosBulkExecutionOptionsHelper.getCosmosBulkExecutionOptionsAccessor();
        CosmosBulkExecutionOptions bulkExecutionOptions = new CosmosBulkExecutionOptions();

        bulkExecutionOptions = accessor.setMaxMicroBatchSize(bulkExecutionOptions, ingestArguments.getMaxMicroBatchSize());
        bulkExecutionOptions = accessor.setTargetedMicroBatchRetryRate(bulkExecutionOptions, ingestArguments.getMinMicroBatchRetryRate(), ingestArguments.getMaxMicroBatchRetryRate());

        // And this one is public for some reason...
        bulkExecutionOptions.setMaxMicroBatchConcurrency(ingestArguments.getMaxMicroBatchConcurrency());
        return bulkExecutionOptions;
    }

    private static Flux<CosmosBulkItemResponse> executeItemOperationsWithErrorHandling(CosmosAsyncContainer container,
                                                                                       Flux<CosmosItemOperation> itemOperations,
                                                                                       CosmosBulkExecutionOptions cosmosBulkExecutionOptions) {
        // Only the first and last few lines are the "execute" bits, all the rest is error handling iff something goes wrong.
        return container.executeBulkOperations(itemOperations, cosmosBulkExecutionOptions).flatMap(operationResponse -> {
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
