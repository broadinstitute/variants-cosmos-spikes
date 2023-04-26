package org.broadinstitute.gvs.azure.cosmos;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Test
public class AvroReaderTest {

    private static final String[] dummyArgvForTesting = {
            "--container", "dummy-container",
            "--database", "dummy-database",
            "--avro-dir", "dummy-avro-dir"
    };

    public void testFindAvroFiles() {
        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets/spanning_samples");
        Assert.assertEquals(avroFiles.size(), 1);
    }

    public void testEndLocationVet() {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString;
        ObjectNode json;

        try {
            // Single alt SNP
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000001,
                        "ref": "A",
                        "alt": "C"
                    }
                    """;

            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000001L);

            // Single alt INDEL
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000001,
                        "ref": "A",
                        "alt": "CT"
                    }
                    """;
            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000002L);

            // Multiple alt SNP
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000001,
                        "ref": "A",
                        "alt": "C,T"
                    }
                    """;

            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000001L);

            // Multiple alt insertion
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000001,
                        "ref": "A",
                        "alt": "CC,TTT"
                    }
                    """;
            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000003L);

            // Multiple alt deletion
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000001,
                        "ref": "AAAA",
                        "alt": "C,TT,GGG"
                    }
                    """;
            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000004L);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void testEndLocationRefRanges() {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonString;
        ObjectNode json;

        try {
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000001,
                        "length": 12
                    }
                    """;

            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000012L);
            // ref ranges without drop state should not have a drop state field
            Iterator<String> fieldNames = json.fieldNames();
            while (fieldNames.hasNext()) {
                if (fieldNames.next().equals("dropState")) {
                    Assert.fail("Unexpectedly found field 'dropState' in a ref ranges document");
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void testDocumentsForAvroPathSpanningSamples() {
        ObjectMapper objectMapper = new ObjectMapper();
        IngestArguments ingestArguments;
        AtomicLong id = new AtomicLong();
        AtomicLong counter = new AtomicLong();

        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets/spanning_samples");

        ingestArguments = IngestArguments.parseArgs(dummyArgvForTesting);
        Assert.assertEquals(ingestArguments.getMaxRecordsPerDocument(), 10000L);

        List<ObjectNode> documents = AvroReader.documentsForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments, id, counter);

        Assert.assertEquals(documents.size(), 2);
        Assert.assertEquals(documents.get(0).get("entries").size(), 88);
        Assert.assertNotNull(documents.get(0).get("location").get("end"));
        Assert.assertEquals(documents.get(1).get("entries").size(), 12);
        Assert.assertNotNull(documents.get(1).get("location").get("end"));
        Assert.assertEquals(counter.get(), 100L);

        String[] args = Arrays.copyOf(dummyArgvForTesting, dummyArgvForTesting.length + 2);
        args[dummyArgvForTesting.length] = "--max-records-per-document";
        args[dummyArgvForTesting.length + 1] = "10";

        ingestArguments = IngestArguments.parseArgs(args);

        id = new AtomicLong();
        counter = new AtomicLong();

        documents = AvroReader.documentsForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments, id, counter);

        Assert.assertEquals(documents.size(), 11);
        for (int i = 0; i < 8; i++) {
            Assert.assertEquals(documents.get(i).get("entries").size(), 10);
            Assert.assertNotNull(documents.get(i).get("location").get("end"));
            Assert.assertEquals(documents.get(i).get("chromosome").asInt(), 24);
        }
        Assert.assertEquals(documents.get(8).get("entries").size(), 8);
        Assert.assertNotNull(documents.get(8).get("location").get("end"));
        Assert.assertEquals(documents.get(8).get("chromosome").asInt(), 24);

        Assert.assertEquals(documents.get(9).get("entries").size(), 10);
        Assert.assertNotNull(documents.get(9).get("location").get("end"));
        Assert.assertEquals(documents.get(9).get("chromosome").asInt(), 1);


        Assert.assertEquals(documents.get(10).get("entries").size(), 2);
        Assert.assertNotNull(documents.get(10).get("location").get("end"));
        Assert.assertEquals(documents.get(10).get("chromosome").asInt(), 1);
        Assert.assertEquals(counter.get(), 100L);
    }

    public void testDocumentsForAvroPathSpanningChromosomes() {
        ObjectMapper objectMapper = new ObjectMapper();
        IngestArguments ingestArguments;
        AtomicLong id = new AtomicLong();
        AtomicLong counter = new AtomicLong();

        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets/spanning_chromosomes");
        ingestArguments = IngestArguments.parseArgs(dummyArgvForTesting);
        Assert.assertEquals(ingestArguments.getMaxRecordsPerDocument(), 10000L);

        List<ObjectNode> documents = AvroReader.documentsForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments, id, counter);

        Assert.assertEquals(documents.size(), 2);

        for (JsonNode jsonNode : documents.get(0).get("entries")) {
            ObjectNode entry = (ObjectNode) jsonNode;
            Assert.assertEquals(entry.get("location").asLong() / AvroReader.CHROMOSOME_MULTIPLIER, 11);
        }

        for (JsonNode jsonNode : documents.get(1).get("entries")) {
            ObjectNode entry = (ObjectNode) jsonNode;
            Assert.assertEquals(entry.get("location").asLong() / AvroReader.CHROMOSOME_MULTIPLIER, 12);
        }
    }

    public void testRefRangesWithDropState() {
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicLong id = new AtomicLong();
        AtomicLong counter = new AtomicLong();

        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/ref_ranges/spanning_samples");

        String[] argv = Arrays.copyOf(dummyArgvForTesting, dummyArgvForTesting.length + 2);
        argv[dummyArgvForTesting.length] = "--drop-state";
        argv[dummyArgvForTesting.length + 1] = "4";
        IngestArguments ingestArguments = IngestArguments.parseArgs(argv);

        List<ObjectNode> documents = AvroReader.documentsForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments, id, counter);

        Assert.assertEquals(documents.size(), 2);
        Assert.assertEquals(counter.get(), 100L);
        Assert.assertEquals(documents.get(0).get("entries").size(), 49);
        Assert.assertEquals(documents.get(0).get("chromosome").asInt(), 24);
        Assert.assertEquals(documents.get(1).get("entries").size(), 11);
        Assert.assertEquals(documents.get(1).get("chromosome").asInt(), 1);

        for (ObjectNode objectNode : documents) {
            Assert.assertEquals(objectNode.get("dropState").asText(), "4");
            Iterator<JsonNode> entries = objectNode.get("entries").iterator();
            // '4' states should have been dropped, we don't expect to see any in this stream.
            Stream<String> shouldBeDrops = Stream.generate(() -> null)
                    .takeWhile(x -> entries.hasNext())
                    .map(n -> entries.next().get("state").asText())
                    .filter("4"::equals);
            Assert.assertTrue(shouldBeDrops.findAny().isEmpty());
        }
    }

    @Test
    public void testFormatAvroRecordForCosmos() throws JsonProcessingException {
        String unoptimizedString = """
                {
                  "sample_id": 1,
                  "non_null": "something",
                  "null": null,
                  "also_non_null": "something else"
                }
                """;
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = (ObjectNode) mapper.readTree(unoptimizedString);

        Iterator<Map.Entry<String, JsonNode>> beforeFields = objectNode.fields();
        Set<String> beforeFieldNames = new HashSet<>();
        Stream.generate(() -> null)
                .takeWhile(x -> beforeFields.hasNext())
                .map(n -> beforeFields.next().getKey())
                .forEach(beforeFieldNames::add);
        Assert.assertEquals(beforeFieldNames, Set.of("sample_id", "non_null", "null", "also_non_null"));

        AvroReader.formatAvroRecordForCosmos(objectNode);
        Iterator<Map.Entry<String, JsonNode>> afterFields = objectNode.fields();
        Set<String> afterFieldNames = new HashSet<>();
        Stream.generate(() -> null)
                .takeWhile(x -> afterFields.hasNext())
                .map(n -> afterFields.next().getKey())
                .forEach(afterFieldNames::add);

        Assert.assertEquals(afterFieldNames, Set.of("non_null", "also_non_null"));
    }
}
