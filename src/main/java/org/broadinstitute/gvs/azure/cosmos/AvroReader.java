package org.broadinstitute.gvs.azure.cosmos;

import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class AvroReader {

    private static final Logger logger = LoggerFactory.getLogger(AvroReader.class);

    public static final long CHROMOSOME_MULTIPLIER = 1000000000000L;

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

        // Check if this is a reference block record with a 'length' property:
        IntNode length = (IntNode) record.get("length");
        if (length != null) {
            return location.asLong() + length.asLong() - 1;
        }

        // If the record does not represent a reference block it must represent a variant. This could be a SNP,
        // insertion, or deletion. There can be multiple alts separated by commas, find the longest to compare with the
        // length of the ref to get the range of bases covered by this variant.
        long refLength = record.get("ref").asText().length();

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        int maxAltLength = Arrays.stream(record.get("alt").asText().split(",")).map(String::length).max(Integer::compareTo).get();

        return location.asLong() + Math.max(refLength, maxAltLength) - 1;
    }

    private static void finishCurrentDocument(ObjectNode currentDocument, long currentMaxLocation, String dropState) {
        ObjectNode location = (ObjectNode) currentDocument.get("location");
        location.set("end", new LongNode(currentMaxLocation));
        if (dropState != null) {
            currentDocument.put("dropState", dropState);
        }
    }

    @VisibleForTesting
    static List<ObjectNode> documentsForAvroPath(
            ObjectMapper objectMapper, Path avroPath, IngestArguments ingestArguments,
            AtomicLong recordCounter, AtomicLong documentCounter) {
        // Where the Cosmos JSON serialization magic happens:
        // https://github.com/Azure/azure-sdk-for-java/blob/80b12e48aeb6ad2f49e86643dfd7223bde7a9a0c/sdk/cosmos/azure-cosmos/src/main/java/com/azure/cosmos/implementation/JsonSerializable.java#L255

        File avroFile = new File(avroPath.toString());
        GenericDatumReader<?> reader = new GenericDatumReader<>();
        List<ObjectNode> documentList = new ArrayList<>();
        ObjectNode currentDocument = null;
        ArrayNode currentRecordArray = null;
        Long currentSampleId = null;
        long currentMaxLocation = -1L;
        short currentChromosome = -1;
        ArrayNode avroSchema = null;
        String dropState = ingestArguments.getDropState();

        String documentJsonTemplate = """
                {
                     "id": "%d",
                     "sample_id" : %d,
                     "chromosome": %d,
                     "location" : {
                         "start" : %d
                     },
                     "schema": [],
                     "entries" : []
                }
                """;

        try {
            try (DataFileReader<?> dataFileReader = new DataFileReader<>(avroFile, reader)) {
                for (Object avroRecord : dataFileReader) {
                    Long longRecordCounter = recordCounter.incrementAndGet();
                    ObjectNode record = (ObjectNode) objectMapper.readTree(avroRecord.toString());

                    if (dropState != null) {
                        String state = record.get("state").asText();
                        // Drop this record if its state matches the drop state.
                        if (state.equals(dropState)) {
                            if (longRecordCounter % ingestArguments.getNumProgress() == 0L) logger.info(longRecordCounter + "...");
                            continue;
                        }
                    }
                    Long sampleId = record.get("sample_id").asLong();
                    Short chromosome = (short) (record.get("location").asLong() / CHROMOSOME_MULTIPLIER);
                    formatAvroRecordForCosmos(record);

                    if (sampleId.equals(currentSampleId) && chromosome.equals(currentChromosome) &&
                            currentRecordArray.size() < ingestArguments.getMaxRecordsPerDocument()) {
                        // Add to current document
                        currentRecordArray.add(record);
                        currentMaxLocation = Math.max(currentMaxLocation, calculateEndLocation(record));
                    } else {
                        // Make a new document.
                        if (currentDocument != null) {
                            // Finish off the current document if there is one.
                            finishCurrentDocument(currentDocument, currentMaxLocation, dropState);
                        }
                        if (avroSchema == null) {
                            avroSchema = (ArrayNode) objectMapper.readTree(reader.getSchema().toString()).get("fields");
                        }

                        // On to the next document.
                        String currentDocumentText = String.format(
                                documentJsonTemplate, documentCounter.incrementAndGet(),
                                sampleId, chromosome, record.get("location").asLong());

                        currentDocument = (ObjectNode) objectMapper.readTree(currentDocumentText);
                        documentList.add(currentDocument);

                        ArrayNode schema = (ArrayNode) currentDocument.get("schema");
                        schema.addAll(avroSchema);

                        currentRecordArray = (ArrayNode) currentDocument.get("entries");
                        currentRecordArray.add(record);
                        currentSampleId = sampleId;
                        currentChromosome = chromosome;
                        currentMaxLocation = calculateEndLocation(record);
                    }
                    if (longRecordCounter % ingestArguments.getNumProgress() == 0L) logger.info(longRecordCounter + "...");
                }
                finishCurrentDocument(currentDocument, currentMaxLocation, dropState);
            }
            return documentList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static void formatAvroRecordForCosmos(ObjectNode record) {
        // The `sample_id` field will become redundant; the containing document will have the same sample_id for every
        // individual variant or ref range item.
        record.remove("sample_id");

        // Iterate the fields of the record looking for anything null-valued. Remove these key / values as they only
        // take up space. The schema is in the containing document which would allow for them to be restored when
        // pulling out of Cosmos.
        Iterator<Map.Entry<String, JsonNode>> it = record.fields();
        List<String> fieldsToRemove = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> next = it.next();
            if (next.getValue().isNull()) {
                fieldsToRemove.add(next.getKey());
            }
        }
        record.remove(fieldsToRemove);
    }

    public static Flux<CosmosItemOperation> itemFluxFromAvroPath(
            ObjectMapper objectMapper, Path avroPath, IngestArguments ingestArguments, AtomicLong recordCounter, AtomicLong documentCounter) {

        List<ObjectNode> documents = documentsForAvroPath(objectMapper, avroPath, ingestArguments, recordCounter, documentCounter);
        return Flux.fromIterable(documents).map(
                document -> CosmosBulkOperations.getCreateItemOperation(
                        document, new PartitionKey(document.get("sample_id").longValue())));
    }
}
