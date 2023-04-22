package org.broadinstitute.gvs.azure.cosmos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AvroReaderTest {

    private static final String[] dummyArgvForTesting = {
            "--container", "dummy-container",
            "--database", "dummy-database",
            "--avro-dir", "dummy-avro-dir"
    };

    @Test
    public void testFindAvroFiles() {
        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets");
        Assert.assertEquals(avroFiles.size(), 1);

    }

    @Test
    public void testObjectNodesForAvroPath() {
        ObjectMapper objectMapper = new ObjectMapper();
        IngestArguments ingestArguments;

        List<Path> avroFiles = AvroReader.findAvroPaths("src/test/resources/vets");

        ingestArguments = IngestArguments.parseArgs(dummyArgvForTesting);
        Assert.assertEquals(ingestArguments.getMaxRecordsPerDocument(), 10000L);

        List<ObjectNode> objectNodes = AvroReader.objectNodesForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments);

        Assert.assertEquals(objectNodes.size(), 2);
        Assert.assertEquals(objectNodes.get(0).get("entries").size(), 88);
        Assert.assertEquals(objectNodes.get(1).get("entries").size(), 12);

        String [] args = Arrays.copyOf(dummyArgvForTesting, dummyArgvForTesting.length + 2);
        args[dummyArgvForTesting.length] = "--max-records-per-document";
        args[dummyArgvForTesting.length + 1] = "10";

        ingestArguments = IngestArguments.parseArgs(args);

        objectNodes = AvroReader.objectNodesForAvroPath(
                objectMapper, avroFiles.get(0), ingestArguments);

        Assert.assertEquals(objectNodes.size(), 11);
        for (int i = 0; i < 8; i++) {
            Assert.assertEquals(objectNodes.get(i).get("entries").size(), 10);
        }
        Assert.assertEquals(objectNodes.get(8).get("entries").size(), 8);
        Assert.assertEquals(objectNodes.get(9).get("entries").size(), 10);
        Assert.assertEquals(objectNodes.get(10).get("entries").size(), 2);
    }
}
