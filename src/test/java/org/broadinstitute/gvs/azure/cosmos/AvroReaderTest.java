package org.broadinstitute.gvs.azure.cosmos;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Test
public class AvroReaderTest {

    private static final String[] dummyArgvForTesting = {
            "--container", "dummy-container",
            "--database", "dummy-database",
            "--avro-dir", "dummy-avro-dir"
    };

    public void testFindAvroFiles() {
        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets");
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
                        "location": 1000000000000,
                        "ref": "A",
                        "alt": "C"
                    }
                    """;

            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000000L);

            // Single alt INDEL
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000000,
                        "ref": "A",
                        "alt": "CT"
                    }
                    """;
            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000001L);

            // Multiple alt SNP
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000000,
                        "ref": "A",
                        "alt": "C,T"
                    }
                    """;

            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000000L);

            // Multiple alt insertion
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000000,
                        "ref": "A",
                        "alt": "CC,TTT"
                    }
                    """;
            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000002L);

            // Multiple alt deletion
            jsonString = """
                    {
                        "sample_id": "1",
                        "location": 1000000000000,
                        "ref": "AAAA",
                        "alt": "C,TT,GGG"
                    }
                    """;
            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000003L);
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
                        "location": 1000000000000,
                        "length": 12
                    }
                    """;

            json = (ObjectNode) objectMapper.readTree(jsonString);
            Assert.assertEquals(AvroReader.calculateEndLocation(json), 1000000000011L);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void testObjectNodesForAvroPath() {
        ObjectMapper objectMapper = new ObjectMapper();
        IngestArguments ingestArguments;
        AtomicLong id = new AtomicLong();

        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets");

        ingestArguments = IngestArguments.parseArgs(dummyArgvForTesting);
        Assert.assertEquals(ingestArguments.getMaxRecordsPerDocument(), 10000L);

        List<ObjectNode> objectNodes = AvroReader.objectNodesForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments, id);

        Assert.assertEquals(objectNodes.size(), 2);
        Assert.assertEquals(objectNodes.get(0).get("entries").size(), 88);
        Assert.assertNotNull(objectNodes.get(0).get("location").get("end"));
        Assert.assertEquals(objectNodes.get(1).get("entries").size(), 12);
        Assert.assertNotNull(objectNodes.get(1).get("location").get("end"));

        String[] args = Arrays.copyOf(dummyArgvForTesting, dummyArgvForTesting.length + 2);
        args[dummyArgvForTesting.length] = "--max-records-per-document";
        args[dummyArgvForTesting.length + 1] = "10";

        ingestArguments = IngestArguments.parseArgs(args);

        id = new AtomicLong();

        objectNodes = AvroReader.objectNodesForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments, id);

        Assert.assertEquals(objectNodes.size(), 11);
        for (int i = 0; i < 8; i++) {
            Assert.assertEquals(objectNodes.get(i).get("entries").size(), 10);
            Assert.assertNotNull(objectNodes.get(i).get("location").get("end"));
        }
        Assert.assertEquals(objectNodes.get(8).get("entries").size(), 8);
        Assert.assertNotNull(objectNodes.get(8).get("location").get("end"));

        Assert.assertEquals(objectNodes.get(9).get("entries").size(), 10);
        Assert.assertNotNull(objectNodes.get(9).get("location").get("end"));

        Assert.assertEquals(objectNodes.get(10).get("entries").size(), 2);
        Assert.assertNotNull(objectNodes.get(10).get("location").get("end"));
    }
}
