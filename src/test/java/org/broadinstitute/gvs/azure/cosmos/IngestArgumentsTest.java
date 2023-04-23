package org.broadinstitute.gvs.azure.cosmos;


import com.beust.jcommander.ParameterException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class IngestArgumentsTest {
    @Test(expectedExceptions = {ParameterException.class},
            expectedExceptionsMessageRegExp = "The following options are required: \\[--database], \\[--avro-dir]")
    public void containerOnly() {
        IngestArguments.parseArgs(new String[] {"--container", "mycontainer"});
    }

    @Test(expectedExceptions = {ParameterException.class},
            expectedExceptionsMessageRegExp = "The following options are required: \\[--avro-dir], \\[--container]")
    public void databaseOnly() {
        IngestArguments.parseArgs(new String[] {"--database", "mydatabase"});
    }

    @Test(expectedExceptions = {ParameterException.class},
            expectedExceptionsMessageRegExp = "The following options are required: \\[--database], \\[--container]")
    public void avroDirOnly() {
        IngestArguments.parseArgs(new String[] {"--avro-dir", "myavros"});
    }


    public void validInvocationWithDefaults() {
        IngestArguments args = IngestArguments.parseArgs(
                new String[] {"--container", "mycontainer", "--database", "mydatabase", "--avro-dir", "myavros"});
        Assert.assertEquals(args.getContainer(), "mycontainer");
        Assert.assertEquals(args.getDatabase(), "mydatabase");
        Assert.assertEquals(args.getNumRecords(), IngestArguments.MAX_RECORDS);
        Assert.assertEquals(args.getNumProgress(), IngestArguments.NUM_PROGRESS);
        Assert.assertEquals(args.getMaxRecordsPerDocument(), IngestArguments.MAX_RECORDS_PER_DOCUMENT);
    }

    public void validInvocationWithOverrides() {
        IngestArguments args = IngestArguments.parseArgs(
                new String[] {
                        "--container", "mycontainer", "--database", "mydatabase", "--avro-dir", "myavros",
                        "--max-records", "99999", "--num-progress", "99", "--max-records-per-document", "999"});
        Assert.assertEquals(args.getContainer(), "mycontainer");
        Assert.assertEquals(args.getDatabase(), "mydatabase");
        Assert.assertEquals(args.getNumRecords(), 99999L);
        Assert.assertEquals(args.getNumProgress(), 99L);
        Assert.assertEquals(args.getMaxRecordsPerDocument(), 999L);
    }
}
