package org.broadinstitute.gvs.azure.cosmos;

import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
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
import java.util.Arrays;
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
    static long calculateEndLocation(ObjectNode record) {
        LongNode location = (LongNode) record.get("location");
        IntNode length = (IntNode) record.get("length");

        // Reference block record?
        if (length != null) {
            return location.asLong() + length.asLong() - 1;
        }

        // If the record does not represent a reference block it must represent a variant.
        // We could be looking at a SNP, insertion, or deletion. There can be multiple alts separated by commas, find
        // the longest and the shortest.
        long refLength = record.get("ref").asText().length();
        Object[] sortedAltLengths = Arrays.stream(record.get("alt").asText().split(",")).map(String::length).sorted().toArray();
        int minAltLength = (Integer) sortedAltLengths[0];
        int maxAltLength = (Integer) sortedAltLengths[sortedAltLengths.length - 1];

        long delta = Math.max(Math.abs(refLength - maxAltLength), Math.abs(refLength - minAltLength));

        return location.asLong() + delta;
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
        long currentMaxLocation = -1L;
        List<ObjectNode> documentList = new ArrayList<>();
        ObjectNode currentDocument = null;
        ArrayNode currentArrayNode = null;
        Long currentSampleId = null;

        try {
            try (DataFileReader<?> dataFileReader = new DataFileReader<>(file, reader)) {
                for (Object record : dataFileReader) {
                    counter = counter + 1L;
                    ObjectNode objectNodeForAvroRecord = (ObjectNode) objectMapper.readTree(record.toString());
                    Long sampleId = objectNodeForAvroRecord.get("sample_id").asLong();
                    if (sampleId.equals(currentSampleId) && currentArrayNode.size() < ingestArguments.getMaxRecordsPerDocument()) {
                        currentArrayNode.add(objectNodeForAvroRecord);
                        currentMaxLocation = Math.max(currentMaxLocation, calculateEndLocation(objectNodeForAvroRecord));
                    } else {
                        if (currentDocument != null) {
                            // Write the end location for the now-completed document.
                            ObjectNode location = (ObjectNode) currentDocument.get("location");
                            location.set("end", new LongNode(currentMaxLocation));
                        }

                        // On to the next document.
                        id = id + 1;
                        String jsonTemplate = """
                                {
                                     "id": "%d",
                                     "sample_id" : %d,
                                     "location" : {
                                         "start" : %d
                                     },
                                     "entries" : []
                                }
                                """;
                        currentDocument = (ObjectNode) objectMapper.readTree(
                                String.format(jsonTemplate, id, sampleId, objectNodeForAvroRecord.get("location").asLong()));
                        documentList.add(currentDocument);

                        currentArrayNode = (ArrayNode) currentDocument.get("entries");
                        currentArrayNode.add(objectNodeForAvroRecord);
                        currentSampleId = sampleId;
                        currentMaxLocation = calculateEndLocation(objectNodeForAvroRecord);
                    }
                    if (counter % ingestArguments.getNumProgress() == 0L) logger.info(counter + "...");
                }
                // Write the end location for the now-completed document.
                ObjectNode location = (ObjectNode) currentDocument.get("location");
                location.set("end", new LongNode(currentMaxLocation));
            }
            return documentList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Flux<CosmosItemOperation> itemFluxFromAvroPath(ObjectMapper objectMapper, Path path, IngestArguments ingestArguments) {
        List<ObjectNode> documentList = objectNodesForAvroPath(objectMapper, path, ingestArguments);
        return Flux.fromIterable(documentList).map(
                document -> CosmosBulkOperations.getCreateItemOperation(
                        document, new PartitionKey(document.get("sample_id").longValue())));
    }
}
