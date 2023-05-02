package org.broadinstitute.gvs.azure.cosmos;

import ch.qos.logback.classic.Level;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class CosmosIngest {

    public static void main(String[] argv) {
        configureLogging();
        CosmosEndpointAndKey endpointAndKey = CosmosEndpointAndKey.fromEnvironment();
        IngestArguments ingestArguments = IngestArguments.parseArgs(argv);
        List<Path> avroPaths = AvroReader.findAvroPaths(ingestArguments.getAvroDir());

        try (CosmosAsyncClient client = buildClient(endpointAndKey)) {
            CosmosAsyncContainer container = client.
                    getDatabase(ingestArguments.getDatabase()).
                    getContainer(ingestArguments.getContainer());

            loadAvroFiles(container, avroPaths, ingestArguments);
        }
    }

    public static void loadAvroFiles(CosmosAsyncContainer container, Iterable<Path> avroPaths, IngestArguments ingestArguments) {
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicLong recordCounter = new AtomicLong();
        AtomicLong documentCounter = new AtomicLong();

        for (Path avroPath : avroPaths) {
            BulkWriter bulkWriter = new BulkWriter(container);

            AvroReader.itemStreamFromAvroPath(objectMapper, avroPath, ingestArguments, recordCounter, documentCounter).
                    forEach(bulkWriter::scheduleWrites);
            bulkWriter.execute().subscribe();
        }
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
