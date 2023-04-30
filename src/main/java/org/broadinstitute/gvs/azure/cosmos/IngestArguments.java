package org.broadinstitute.gvs.azure.cosmos;

import com.azure.cosmos.implementation.batch.BatchRequestResponseConstants;
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

    public String getDropState() {
        return dropState;
    }

    public Integer getSubmissionBatchSize() {
        return submissionBatchSize;
    }

    public boolean isContinuousFlux() {
        return continuousFlux;
    }

    public Integer getTargetThroughput() {
        return targetThroughput;
    }

    public Integer getMaxMicroBatchSize() {
        return maxMicroBatchSize;
    }

    public Integer getMaxMicroBatchConcurrency() {
        return maxMicroBatchConcurrency;
    }

    public Double getMaxMicroBatchRetryRate() {
        return maxMicroBatchRetryRate;
    }

    public Double getMinMicroBatchRetryRate() {
        return minMicroBatchRetryRate;
    }

    public Integer getMaxMicroBatchIntervalMillis() {
        return maxMicroBatchIntervalMillis;
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

    @Parameter(names = {"--drop-state"}, description = "If a record has a 'state' property with this value specified it will be omitted from the containing document")
    private String dropState;

    @Parameter(names = {"--submission-batch-size"}, description = "The number of documents to submit to Cosmos in a single batch")
    private Integer submissionBatchSize = 100;

    @Parameter(names = {"--continuous-flux"}, description = "Whether to submit to Cosmos file-by-file (default) or in a continuous Flux")
    private boolean continuousFlux = false;

    @Parameter(names = {"--target-throughput"}, description = "Value to specify for Cosmos container local target throughput")
    private Integer targetThroughput;

    @Parameter(names = {"--max-micro-batch-size"}, description = "CosmosBulkExecutionOptions micro batch size")
    private Integer maxMicroBatchSize = BatchRequestResponseConstants.MAX_OPERATIONS_IN_DIRECT_MODE_BATCH_REQUEST;

    @Parameter(names = {"--max-micro-batch-concurrency"}, description = "CosmosBulkExecutionOptions micro batch concurrency")
    private Integer maxMicroBatchConcurrency = BatchRequestResponseConstants.DEFAULT_MAX_MICRO_BATCH_CONCURRENCY;

    @Parameter(names = {"--max-micro-batch-retry-rate"}, description = "CosmosBulkExecutionOptions max micro batch retry rate")
    private Double maxMicroBatchRetryRate = BatchRequestResponseConstants.DEFAULT_MAX_MICRO_BATCH_RETRY_RATE;

    @Parameter(names = {"--min-micro-batch-retry-rate"}, description = "CosmosBulkExecutionOptions min micro batch retry rate")
    private Double minMicroBatchRetryRate = BatchRequestResponseConstants.DEFAULT_MIN_MICRO_BATCH_RETRY_RATE;

    @Parameter(names = {"--min-micro-batch-interval-millis"}, description = "CosmosBulkExecutionOptions min micro batch retry rate")
    private Integer maxMicroBatchIntervalMillis = BatchRequestResponseConstants.DEFAULT_MAX_MICRO_BATCH_INTERVAL_IN_MILLISECONDS;

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
