# How the CDC connector works

Reference for the mechanics behind [Part 2](../README.md#part-2--watch-the-connectors-change-feed). You
do not need any of it to finish the lab. Read it when you want to know why the pipeline is shaped this
way, or when you are planning a real Openflow deployment.

The simulator reproduces each behaviour below faithfully, so what you see here is what a real
[Openflow Postgres CDC connector](https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/postgres/about)
does.

## Who creates the destination table

| Ingestion path | Who creates the target table |
|---|---|
| **Openflow CDC connector** | The **connector**. It creates the schemas and destination tables matching the source tables; you point it at a source and the objects appear. |
| **Snowpipe Streaming client** | **You do.** The SDK auto-creates the *pipe*, never the table. |

A managed connector provisions its own destination; a streaming application does not. That decides who
owns your schema, and it is why you write `STATION_TELEMETRY`'s DDL yourself in Part 1 but never write
the CDC destination's.

## The journal's three event types

The connector does not write changes straight to the destination. It appends every change event to a
**journal** table, and an `APPEND_ONLY` stream on that journal feeds a MERGE that maintains the
destination.

| `EVENT_TYPE` | What it carries |
|---|---|
| `IncrementalInsertRows` | every `PAYLOAD__*` populated |
| `IncrementalUpdateRows` | `PAYLOAD__*` holds the **new** values; `PRIMARY_KEY__*` the **old** key |
| `IncrementalDeleteRows` | every `PAYLOAD__*` is **NULL**; the key alone identifies the row |

The delete case is why the MERGE's insert branch needs
`IFF(EVENT_TYPE='IncrementalDeleteRows', PRIMARY_KEY__INSPECTION_ID, PAYLOAD__INSPECTION_ID)`: on a
delete there is no payload to read the key from.

`IncrementalUpdateRows` is what makes Part 5's recovery work. An inspector overturning a FAIL to PASS
arrives as an update, and the new values ride in the payload.

## Soft deletes

The connector never removes a row. It sets `_SNOWFLAKE_DELETED = TRUE` and leaves it in place, which is
why every Dynamic Table built on the destination needs `WHERE NOT _SNOWFLAKE_DELETED`. Omit it and
retracted rows count for ever.

## The merge gate

There is **no Snowflake task**. The connector's merge processor runs inside the connector runtime and
issues the MERGE itself over its own connection, on a CRON eligibility gate — second `:00` of every
minute by default. The producer does the same thing on the same gate.

So the journal always leads the destination, by up to a minute's worth of change events. That gap is a
schedule you chose, not a throughput limit.

## `SF_METADATA` holds a string, not an object

`SF_METADATA` is typed `VARIANT` but contains a JSON **string**, because that is what the connector
writes. `TYPEOF(SF_METADATA)` returns `VARCHAR` and `SF_METADATA:offset_token` returns `NULL`. Parse it
first: `PARSE_JSON(SF_METADATA::STRING):offset_token`.

## Auditing merges with the query tag

Every merge carries a `QUERY_TAG` naming the application, the operation and the merge strategy:

```json
{"application":"SNOWFLAKE_OPENFLOW","operation":"cdc.merge.full_values","strategy":"full_values_snowflake_managed"}
```

Filtering `QUERY_HISTORY` on that tag is how you audit a real deployment. Expect roughly one MERGE per
minute, each starting at second `:00` and finishing in a second or two: many short merges on a schedule,
not one long-running one.

## The journal is connector-internal

Its name carries a generation counter (`QUALITY_INSPECTIONS_JOURNAL_1787700000_1`) and the connector
prunes it on its own retention schedule. Build on the destination table, never on the journal.
