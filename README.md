A collection of DSP Variants Team spikes for Azure Cosmos DB.

# VS-890 Translation of the original Cosmos ingest spike from Ammonite to Java.

Includes initial Gradle and GitHub Actions setup to run unit tests.

# VS-893 Group records for improved ingest performance.

Groups variant and reference rows into fewer higher-level documents. Groupings by 6000 for vets and 40000 for reference
ranges (larger numbers produce HTTP 413 "Request size is too large" errors from Cosmos). This version of the code
requires slightly modified `EXPORT DATA` statements to order data by `sample_id` and `location`:

`ref_ranges`:

```
EXPORT DATA
  OPTIONS( uri='gs://bucket/path/to/avros/ref_ranges/ref_ranges_001/ref_ranges_001_*.avro',
    format='AVRO',
    compression='SNAPPY') AS
SELECT
  r.sample_id AS sample_id,
  location,
  length,
  state
FROM
  `gvs-internal.quickstart_dataset.ref_ranges_001` r
INNER JOIN
  `gvs-internal.quickstart_dataset.sample_info` s
ON
  s.sample_id = r.sample_id
WHERE
  withdrawn IS NULL
  AND is_control = FALSE
ORDER BY
  sample_id,
  location
```

`vets`:

```
EXPORT DATA
  OPTIONS( uri='gs://bucket/path/to/avros/vets/vet_001/vet_001_*.avro',
    format='AVRO',
    compression='SNAPPY') AS
SELECT
  v.sample_id AS sample_id,
  location,
  ref,
  alt,
  AS_RAW_MQ,
  AS_RAW_MQRankSum,
  QUALapprox,
  AS_QUALapprox,
  AS_RAW_ReadPosRankSum,
  AS_SB_TABLE,
  AS_VarDP,
  call_GT,
  call_AD,
  call_GQ,
  call_PGT,
  call_PID,
  call_PL
FROM
  `gvs-internal.quickstart_dataset.vet_001` v
INNER JOIN
  `gvs-internal.quickstart_dataset.sample_info` s
ON
  s.sample_id = v.sample_id
WHERE
  withdrawn IS NULL
  AND is_control = FALSE
ORDER BY
  sample_id,
  location
```

To realize ~10x performance gains relative to the VS-890 code, the indexing policy on the target Cosmos container needs
to be changed from `automatic` to `none` (this code will work without updating the Cosmos indexing policy but it won't
be much faster than the VS-890 baseline). The updated indexing policy should look like:

```
{
    "indexingMode": "none",
    "automatic": false,
    "includedPaths": [
    ],
    "excludedPaths": [
    ]
}
```

# VS-906 Cosmos DB Serverless Exploration

[Cosmos DB serverless](https://learn.microsoft.com/en-us/azure/cosmos-db/serverless) is restricted to [50 GB per
container and 5000 RU/s throughput](https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#serverless). There is a
[preview 1 TB container](https://learn.microsoft.com/en-us/azure/cosmos-db/serverless-1tb) offering in which I have
enrolled the Variants subscription which will offer higher throughput as storage grows.

An ingest run with Quickstart data using the code from this spike consumed 6.65 M RU for reference data and 10.36
M RU for variant data. Run times were ~32 minutes and ~37 minutes, respectively, for a total of ~69 minutes. RU consumption cost was
```
(6.65 + 10.36 =~ 17M RU * $0.25 / RU) = $4.25
```

or $0.425 per sample. Storage cost was

```
10.03 + 13.13 = 23.16 * $0.25 GB / month = $5.79 GB / month
```

or $0.58 / sample * month.

After creating indexes with this specification the UI did not report any change in storage amount (or cost):

```
{
    "indexingMode": "consistent",
    "automatic": true,
    "includedPaths": [
        {
            "path": "/sample_id/*"
        },
        {
            "path": "/location/*"
        }
    ],
    "excludedPaths": [
        {
            "path": "/*"
        }
    ]
}
```

Invocations on a `Standard_E4-2ads_v5` VM looked like:

```
java -Xms2g -Xmx26g -jar build/libs/variantstore-*.jar --database cosmos-gvs-serverless --container ref_ranges \
   --avro-dir /mnt/data/avros-sample-location/ref_ranges/ref_ranges_001/ --max-records-per-document 40000 --drop-state 4
```

```
java -Xms2g -Xmx26g -jar build/libs/variantstore-*.jar --database cosmos-gvs-serverless --container vets \
  --avro-dir /mnt/data/avros-sample-location/vets/vet_001/ --max-records-per-document 6000
```

## Improvements in VS-906

* Support for references drop states with `--drop-state` parameter
* Always split Cosmos documents on chromosome boundaries
* Parameter `--submission-batch-size` to control the maximum size of document batches sent to Cosmos at one time.
* Parameter `--continuous-flux` to turn on more efficient (but more crashy on low throughput) continuous-Flux data loading.
* Support for various Cosmos parameters. I experimented with all of these but none seemed to help with the high numbers of 429 statuses seen in Cosmos Insights:
  * `--target-throughput`: Value to specify for Cosmos container local target throughput
  * `--max-micro-batch-size`: CosmosBulkExecutionOptions micro batch size
  * `--micro-batch-concurrency`: CosmosBulkExecutionOptions micro batch concurrency
  * `--max-micro-batch-retry-rate`: CosmosBulkExecutionOptions max micro batch retry rate
  * `--min-micro-batch-retry-rate`: CosmosBulkExecutionOptions min micro batch retry rate
  * `--min-micro-batch-interval-millis`: CosmosBulkExecutionOptions retry rate in milliseconds
* Lots of general cleanup
