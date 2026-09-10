# Adaptive Multi-Value Keyword Search Functional Test Plan and Execution Report - 2026-09-02

## Executive summary

This document defines and records end-to-end validation for adaptive multi-value keyword indexing and search. The final rerun used OpenSearch `3.9.0-SNAPSHOT` at base commit `06bb9df5f6b8` and the production `/_plugins/_ppl` endpoint. All supported projection, fetch, merge, sorting, single-shard GROUP BY, and explicit `mvexpand` scenarios passed. Two tracked gaps remain: `list()` and `values()` reject ARRAY input in the SQL frontend, and distributed LIST GROUP BY loses its partial-plan output schema.

Execution found two additional problems. The release native library contained stale code, which caused mixed-schema reads and native merge to fail. A forced Cargo rebuild resolved that problem. Real-PPL GROUP BY also retained ARRAY types in a parent Calcite projection after expansion changed the grouping key to a scalar. The fix retypes parent projection references and adds a converter regression.

## Scope

The plan covers the combined behavior from:

- Adaptive scalar-to-LIST keyword promotion.
- Physical LIST merge and fixed-MIN index sorting.
- Mixed-generation search and derived-source reads.
- Query sorting by `MIN(list)`.
- LIST aggregation expansion and per-document deduplication.

LIST-element filtering is not yet executable because array-aware filter semantics (execution plan Deliverable 8) are not implemented. The Planned cases section below defines those cases so they activate as Deliverable 8 lands.

## Test environment

Repository: `/local/home/bansvaru/codebase/oss-dev/OpenSearch`

Branch: `multi-value-integration-testing`

OpenSearch build: `3.9.0-SNAPSHOT`, commit `07df641179d251782efe4be8ba04a6b999ef8d66`

Required plugins:

1. `arrow-base`
2. `arrow-flight-rpc`
3. `analytics-engine`
4. `composite-engine`
5. `parquet-data-format`
6. `analytics-backend-datafusion`
7. `analytics-backend-lucene`
8. `dsl-query-executor`
9. `opensearch-job-scheduler`
10. `opensearch-sql-plugin`

The manual server uses only `opensearch-sql-plugin` as its PPL frontend. Installing `test-ppl-frontend` at the same time registers the PPL transport action twice and prevents startup.

## Server preparation

Rebuild the native library after changing Rust. Gradle task invalidation alone does not guarantee that Cargo recompiles a stale release artifact. Verify the resulting binary contains current code before starting the node.

```bash
./gradlew --no-daemon -Dorg.gradle.vfs.watch=false \
  :sandbox:libs:dataformat-native:buildRustLibrary \
  -Dsandbox.enabled=true --rerun-tasks

strings sandbox/libs/dataformat-native/rust/target/release/libopensearch_native.so \
  | grep 'after scalar-to-LIST promotion'
```

Start OpenSearch. Plugin order matters because the manual installer validates dependencies after each installation.

```bash
NATIVE_LIB_DIR=$(pwd)/sandbox/libs/dataformat-native/rust/target/release

./gradlew --no-daemon run -Dsandbox.enabled=true \
  -PinstalledPlugins="['arrow-base','arrow-flight-rpc','analytics-engine','composite-engine','parquet-data-format','analytics-backend-datafusion','analytics-backend-lucene','dsl-query-executor','opensearch-job-scheduler','opensearch-sql-plugin']" \
  -Dtests.jvm.argline="-Djava.library.path=$NATIVE_LIB_DIR -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true -Dopensearch.experimental.feature.transport.stream.enabled=true --add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED" \
  -x javadoc -x test -x missingJavadoc
```

Verify health and plugin inventory:

```bash
curl -sS http://127.0.0.1:9200/
curl -sS 'http://127.0.0.1:9200/_cat/plugins?v'
```

Enable composite routing and scoped page-index reads:

```bash
curl -sS -X PUT http://127.0.0.1:9200/_cluster/settings \
  -H 'Content-Type: application/json' \
  -d '{
    "persistent": {
      "cluster.pluggable.dataformat": "composite",
      "datafusion.scoped_page_index.enabled": true
    }
  }'
```

## Common index settings

Each test index uses these settings unless a case overrides them:

```json
{
  "number_of_replicas": 0,
  "index.refresh_interval": "-1",
  "index.pluggable.dataformat.enabled": true,
  "index.pluggable.dataformat": "composite",
  "index.composite.primary_data_format": "parquet",
  "index.composite.secondary_data_formats": "lucene"
}
```

## Functional cases

### F01: Adaptive promotion and mixed-generation projection

Create a one-shard index with `id` and `tags` mapped as keyword fields. Do not set `multi_value` on `tags`. Index and flush a scalar document, then index and flush a LIST document.

```text
before: {"id":"before","tags":"prod"}
after:  {"id":"after","tags":["prod","error"]}
```

Run:

```json
POST /_plugins/_ppl
{"query":"source=mv_auto_functional | sort id | fields id, tags"}
```

Expected rows:

```text
after  -> ["prod", "error"]
before -> ["prod"]
```

Also verify that `GET /mv_auto_functional/_mapping` publishes `tags.multi_value=true`.

### F02: Indexed execution, QTF fetch, and scoped page index

Run a scalar predicate while projecting the promoted LIST:

```text
source=mv_auto_functional | where id = 'before' | fields id, tags
```

Expected row:

```text
before -> ["prod"]
```

Run a sort and head operation that fetches only the LIST column:

```text
source=mv_auto_functional | sort id | head 1 | fields tags
```

Expected value:

```text
["prod", "error"]
```

Keep `datafusion.scoped_page_index.enabled=true` for both queries.

### F03: Force merge and derived-source GET

Run:

```http
POST /mv_auto_functional/_forcemerge?max_num_segments=1&flush=true
```

Require zero failed shards. Rerun F01 and require identical rows. GET both source documents after merge.

Expected source values:

```text
pre-promotion document  -> ["prod"]
post-promotion document -> ["prod", "error"]
```

### F04: Fixed-MIN query sorting

Create a one-shard index with `tags.multi_value=true` and these index-sort settings:

```json
{
  "index.sort.field": "tags",
  "index.sort.order": "asc",
  "index.sort.missing": "_first"
}
```

Index:

```text
d1      tags=["z","delta"]       MIN=delta
d2      tags=["omega","alpha"]   MIN=alpha
d3      tags=["gamma","beta"]    MIN=beta
missing tags absent
```

Queries and expected identifiers:

```text
source=mv_sort_functional | sort + tags | fields id, tags
missing, d2, d3, d1

source=mv_sort_functional | sort - tags | fields id, tags
d1, d3, d2, missing
```

The displayed arrays must retain ingestion order. Query sorting must not rewrite them as sorted arrays.

### F05: Single-shard LIST GROUP BY

Index:

```text
d1 tags=["a","a","b"], colors=["red","blue"]
d2 tags=["a"],           colors=["red"]
```

Run:

```text
source=mv_group_functional | stats count() as c by tags | sort tags
```

Expected counts:

```text
a=2
b=1
```

Run:

```text
source=mv_group_functional | stats count() as c by tags, colors | sort tags, colors
```

Expected counts:

```text
a:blue=1
a:red=2
b:blue=1
b:red=1
```

These results prove per-document deduplication and Cartesian expansion across two LIST keys.

### F06: Known-blocker and resolved probes

Run each query and record the exact result. The first, second, and fourth queries remain tracked gaps. The `mvexpand` query must pass.

```text
source=mv_known_gaps_functional | stats list(tags) as all
source=mv_known_gaps_functional | stats values(tags) as unique
source=mv_known_gaps_functional | mvexpand tags limit=2 | sort id, tags | fields id, tags
source=mv_known_gaps_functional | stats count() as c by tags | sort tags
```

Use two shards for the last query.

Expected results:

- `list()` returns HTTP 400 because the SQL frontend rejects ARRAY input.
- `values()` returns HTTP 400 because the SQL frontend rejects ARRAY input.
- `mvexpand` returns the per-document elements up to limit 2; the QA fixture returns `d1:a`, `d1:b`, `d2:x`.
- Distributed LIST GROUP BY returns HTTP 400 with `Field reference offset (0) must be less than number of fields in struct (0)`.

## Planned cases for Deliverable 8: array-aware filters and functions

Added 2026-09-03 and updated 2026-09-04. These cases encode the Array-aware filter scope section of the execution plan (https://chorus.aws.dev/doc/xHom3UITmnhz). F07 equality/IN and the direct ARRAY functions are now implemented; F08, F09, and element-wise F11 behavior still require semantic decisions. F13 remains executable.

Shared fixture index `mv_filter_functional` (one shard, `tags` keyword, `multi_value` omitted):

```text
d1 tags=["prod","blue"]
d2 tags=["blue"]
d3 tags="prod"        (indexed and flushed before any array document)
d4 tags=[]
d5 tags=null
d6 tags=[null]
```

### F07: Equality and IN membership

```text
source=mv_filter_functional | where tags = 'prod'  | fields id
source=mv_filter_functional | where tags in ('prod','error') | fields id
```

Expected after Deliverable 8: both return `d1, d3` (any-element membership, pre-promotion scalar included). Also record the decided `!=` contract with one query per decision candidate.

### F08: Range and BETWEEN decision

```text
source=mv_filter_functional | where tags > 'blue' | fields id
```

Expected: the behavior chosen in the scope section (MIN-reduction consistent with sorting, or an explicit rejection error). The test asserts the decided contract, not both.

### F09: LIKE and REGEXP decision

```text
source=mv_filter_functional | where like(tags, 'pro%') | fields id
```

Expected: any-element match returning `d1, d3`, or the documented rejection error, per the scope decision.

### F10: Null contract

```text
source=mv_filter_functional | where isnull(tags)    | fields id
source=mv_filter_functional | where isnotnull(tags) | fields id
```

Expected: the documented contract distinguishing `d5` (null list), `d4` (empty list), and `d6` (list of null). Storage preserves all three shapes; this case pins the query-visible mapping.

### F11: Rejected scalar functions

```text
source=mv_filter_functional | eval u = upper(tags) | fields u
source=mv_filter_functional | parse tags '(?<w>prod.*)' | fields w
```

Expected: explicit error directing the user to `mvexpand`. Assert the error message, not only the status code.

### F12: Aggregation function contracts

```text
source=mv_filter_functional | stats min(tags), max(tags)
source=mv_filter_functional | stats distinct_count(tags)
source=mv_filter_functional | stats count(tags)
source=mv_filter_functional | top 2 tags
```

Expected: `min`/`max` and `distinct_count` follow the decided reduce-or-reject contract; `count(tags)` counts documents with a non-null list (`d1, d2, d3, d4, d6` = 5, pending the F10 null contract for `d4`/`d6`); `top` inherits Deliverable 7 dedup semantics.

### F13: field_type_tolerance interaction (executable now)

Enable `plugins.query.field_type_tolerance=true` on the cluster, rerun F01 and F02, then disable it and rerun both again.

Expected: identical typed ARRAY results in all four runs. The tolerance setting exists to coerce array-shaped data under scalar-declared schemas; the OPTIMIZED path declares `ARRAY` natively, so the setting must have no effect in either state. A difference in any run indicates the response formatter is applying tolerance coercion to typed LIST columns and requires a guard in the sql plugin.

## Executed results

| Case | Result | Observed evidence |
|------|--------|-------------------|
| Server and plugins | PASS | Node reached green health and reported all 10 required plugins. |
| F01 adaptive promotion | PASS | Mapping published `multi_value=true`; both generations returned LIST values. |
| F02 indexed and QTF reads | PASS | Predicate returned `["prod"]`; QTF returned `["prod","error"]`. |
| F03 merge and GET | PASS | Force merge reported one successful shard and zero failures; GET normalized the scalar generation. |
| F04 ASC sorting | PASS | Returned `missing, d2, d3, d1`. |
| F04 DESC sorting | PASS | Returned `d1, d3, d2, missing`. |
| F04 visible order | PASS | Arrays retained original ingestion order. |
| F05 one-key GROUP BY | PASS after fix | Returned `a=2, b=1`. |
| F05 two-key GROUP BY | PASS after fix | Returned all four expected Cartesian pairs. |
| F06 `list()` | EXPECTED BLOCKER | HTTP 400 ARRAY type rejection. |
| F06 `values()` | EXPECTED BLOCKER | HTTP 400 ARRAY type rejection. |
| F06 `mvexpand` | PASS after fix | Correlate and Uncollect are marked for DataFusion; returned `d1:a`, `d1:b`, `d2:x`. |
| F06 distributed GROUP BY | EXPECTED BLOCKER | HTTP 400 due to missing partial-plan schema. |

The external SQL response labels expanded GROUP BY keys as ARRAY while returning scalar string key values. The row values and counts are correct. The response-schema label remains a frontend metadata issue.

### SQL/PPL compatibility sweep — 2026-09-04

The planned F13 tolerance case passed: with `plugins.query.field_type_tolerance` set to `true` and then `false`, F01 projection and F02 scalar-filter queries returned identical typed ARRAY schemas and values.

A 78-query SQL/PPL sweep was executed on a fresh server. After the approved predicate, element-wise function, conditional, literal, and subscript changes, 66 queries return meaningful results, 8 fail explicitly, and 4 remain silent malformed or empty successes. `UNION ALL` and four tolerance probes also passed. Exact fixture inputs, every query, outputs, and remaining ownership are recorded in [MULTI_VALUE_SQL_COMPATIBILITY_REPORT.md](MULTI_VALUE_SQL_COMPATIBILITY_REPORT.md); the final full payload is in `/tmp/mv-final-query-results-array-semantics.json` and the focused 30-case payload is in `/tmp/array-semantics-runtime-results.json`.

The remaining executable blockers are ARRAY input for `list()` and `values()`, silent `SPLIT` and `CHAR_LENGTH` responses, scalar ORDER BY and subquery plan shapes, distributed GROUP BY partial/final stage binding, and response-schema labels for scalar-expanded values. `parse`, `rex`, and `grok` require explicit `mvexpand` by contract.

## Defects resolved during execution

### Stale release native library

The first server run failed mixed reads and force merge with raw scalar/LIST schema-union errors. The checked-in source included scalar-to-LIST promotion, but `strings` on `libopensearch_native.so` showed the old error text. Forcing Cargo to rebuild the Parquet and DataFusion crates produced a new library with the expected promotion text. F01 through F04 passed after restart.

### Parent projection retained ARRAY type after GROUP BY expansion

The real SQL frontend adds a parent `LogicalProject` above LIST GROUP BY. `MultiValueExpandRel` changes the grouping key from ARRAY to scalar, but the parent project retained an ARRAY `RexInputRef`. Calcite rejected the plan with this mismatch:

```text
ref:   VARCHAR ARRAY
input: VARCHAR
```

`MultiValueRelRewriter` now rebuilds affected parent projections using types from the rewritten child. `DataFusionFragmentConvertorTests` includes a regression for this plan shape. Both F05 queries passed through `/_plugins/_ppl` after restart.

## Automated regression validation

Run:

```bash
./gradlew --no-daemon -Dorg.gradle.vfs.watch=false \
  :sandbox:plugins:analytics-backend-datafusion:spotlessJavaCheck \
  :sandbox:plugins:analytics-backend-datafusion:test \
  -Dsandbox.enabled=true \
  --tests 'org.opensearch.be.datafusion.DataFusionFragmentConvertorTests.testListGroupBy*'

./gradlew --no-daemon -Dorg.gradle.vfs.watch=false \
  :sandbox:qa:analytics-engine-rest:integTest \
  -Dsandbox.enabled=true \
  --tests 'org.opensearch.analytics.qa.MultiValueKeywordIT'

./gradlew --no-daemon -Dorg.gradle.vfs.watch=false \
  :sandbox:qa:analytics-engine-rest:integTestNoMerge \
  -Dsandbox.enabled=true \
  --tests 'org.opensearch.analytics.qa.MultiValueKeywordNoMergeIT'
```

Observed results:

- Focused converter tests passed.
- Spotless passed.
- `MultiValueKeywordIT` ran 6 tests with 2 tracked skips, 0 failures, and 0 errors.
- `MultiValueKeywordNoMergeIT` ran 1 test with 0 skips, 0 failures, and 0 errors.
- `git diff --check` passed.

## Exit criteria

The supported feature stack is ready for review when F01 through F05 pass, F06 returns the documented blocker signatures, automated suites report zero unexpected failures, and the test server is stopped after execution. This execution met all four criteria.
