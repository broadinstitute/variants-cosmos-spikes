package org.broadinstitute.gvs.azure.cosmos;

import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class AvroReader {

    private static final Logger logger = LoggerFactory.getLogger(AvroReader.class);

    public static List<Path> findAvroPaths(String avroDir) {
        try {
            try (Stream<Path> files = Files.list(Path.of(avroDir))) {
                return files.filter(p -> p.getFileName().toString().endsWith(".avro")).toList();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static ObjectNode objectNodeFromAvroRecord(ObjectMapper objectMapper, String jsonString) {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            return (ObjectNode) jsonNode;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error reading String as JSON Object", e);
        }
    }

    @VisibleForTesting
    static List<ObjectNode> objectNodesForAvroPath(ObjectMapper objectMapper, Path path, IngestArguments ingestArguments) {
        // Where the Cosmos JSON serialization magic happens:
        // https://github.com/Azure/azure-sdk-for-java/blob/80b12e48aeb6ad2f49e86643dfd7223bde7a9a0c/sdk/cosmos/azure-cosmos/src/main/java/com/azure/cosmos/implementation/JsonSerializable.java#L255
        //

        File file = new File(path.toString());
        GenericDatumReader<?> reader = new GenericDatumReader<>();
        long counter = 0L;
        long id = 0L;
        List<ObjectNode> documentList = new ArrayList<>();
        ArrayNode currentArrayNode = null;
        String currentSampleId = null;

        try {
            try (DataFileReader<?> dataFileReader = new DataFileReader<>(file, reader)) {
                for (Object record : dataFileReader) {
                    counter = counter + 1L;
                    ObjectNode objectNodeForAvroRecord = objectNodeFromAvroRecord(objectMapper, record.toString());
                    String sampleId = objectNodeForAvroRecord.get("sample_id").asText();
                    if (sampleId.equals(currentSampleId) && currentArrayNode.size() < ingestArguments.getMaxRecordsPerDocument()) {
                        currentArrayNode.add(objectNodeForAvroRecord);
                    } else {
                        id = id + 1;
                        String jsonTemplate = """
                                {
                                     "id": %d,
                                     "sample_id" : "%s",
                                     "location" : {
                                         "start" : %d
                                     },
                                     "entries" : []
                                }
                                """;
                        ObjectNode document = (ObjectNode) objectMapper.readTree(
                                String.format(jsonTemplate, id, sampleId, objectNodeForAvroRecord.get("location").asLong()));
                        documentList.add(document);

                        currentArrayNode = (ArrayNode) document.get("entries");
                        currentArrayNode.add(objectNodeForAvroRecord);
                        currentSampleId = sampleId;
                    }
                    if (counter % ingestArguments.getNumProgress() == 0L) logger.info(counter + "...");
                }
            }
            return documentList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Flux<CosmosItemOperation> itemFluxFromAvroPath(ObjectMapper objectMapper, Path path, IngestArguments ingestArguments) {
        List<ObjectNode> documentList = objectNodesForAvroPath(objectMapper, path, ingestArguments);
        return Flux.fromIterable(documentList).
                map(record -> {
                    ObjectNode objectNode = objectNodeFromAvroRecord(objectMapper, record.toString());
                    return CosmosBulkOperations.getCreateItemOperation(
                            objectNode, new PartitionKey(objectNode.get("sample_id").longValue()));
                });
    }
}
