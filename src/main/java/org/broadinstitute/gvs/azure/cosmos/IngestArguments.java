package org.broadinstitute.gvs.azure.cosmos;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;

public class IngestArguments {
    public String getDatabase() {
        return database;
    }

    public String getContainer() {
        return container;
    }

    public Long getNumRecords() {
        return numRecords;
    }

    public Long getNumProgress() {
        return numProgress;
    }

    public String getAvroDir() {
        return avroDir;
    }

    public Long getMaxRecordsPerDocument() {
        return maxRecordsPerDocument;
    }

    @VisibleForTesting
    static final long NUM_PROGRESS = 1000000;

    @VisibleForTesting
    static final long MAX_RECORDS = Long.MAX_VALUE;

    @VisibleForTesting
    static final long MAX_RECORDS_PER_DOCUMENT = 10000L;

    @Parameter(names = {"--database"}, description = "Cosmos database", required = true)
    private String database;

    @Parameter(names = {"--container"}, description = "Cosmos container", required = true)
    private String container;

    @Parameter(names = {"--avro-dir"}, description = "Directory containing Avro files", required = true)
    private String avroDir;

    @Parameter(names = {"--max-records"}, description = "Maximum number of records to load")
    private Long numRecords = MAX_RECORDS;

    @Parameter(names = {"--num-progress"}, description = "Number of records to load between progress messages")
    private Long numProgress = NUM_PROGRESS;


    @Parameter(names = {"--max-records-per-document"}, description = "Maximum number of records to include within a single Cosmos document")
    private Long maxRecordsPerDocument = MAX_RECORDS_PER_DOCUMENT;

    private IngestArguments() {
    }

    public static IngestArguments parseArgs(String [] argv) {
        IngestArguments args = new IngestArguments();
        JCommander.newBuilder().
                addObject(args).
                build().
                parse(argv);
        return args;
    }
}
