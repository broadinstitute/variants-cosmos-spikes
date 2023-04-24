A collection of DSP Variants Team spikes for Azure Cosmos DB.

# VS-890 Translation of the original Cosmos ingest spike from Ammonite to Java.

Includes initial Gradle and GitHub Actions setup to run unit tests.

# VS-893 Group records for improved ingest performance.

Groups variant and reference rows into fewer higher-level documents. Groupings by 5000 for vets and 30000 for reference
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
