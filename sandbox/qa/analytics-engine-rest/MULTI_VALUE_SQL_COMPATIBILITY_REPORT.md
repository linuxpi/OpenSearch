# Multi-Value SQL and PPL Compatibility Report — 2026-09-04

## Result

A fresh Gradle OpenSearch `3.9.0-SNAPSHOT` server ran 78 representative SQL/PPL query forms against a mixed scalar/LIST Parquet index. After the approved ARRAY semantics fixes, the final result is 66 meaningful responses, 8 explicit failures, and 4 silent or malformed HTTP-200 responses. Meaningful coverage increased by 23 of 78 query forms. Five additional probes verified `UNION ALL` and field-type tolerance in both states.

The in-repository fixes validated by this sweep are:

1. `MIN(tags)` and `MAX(tags)` now reduce each LIST to a hidden scalar key before aggregation.
2. `COUNT(DISTINCT tags)` now uses the existing list-aware exact `os_count_distinct` UDAF.
3. Aggregate aliases such as `COUNT(*) AS c` now map safely to Arrow output by guarded ordinal fallback, fixing LIST `GROUP BY ... HAVING`.
4. `mvexpand` now marks and lowers Calcite Correlate/Uncollect plans for DataFusion execution.
5. ARRAY `UNION` now aligns runtime batches to the stream schema and re-materializes nested variable-width columns before Arrow C export.
6. SQL/PPL ARRAY equality and `IN` lower to `ARRAY_CONTAINS`; `NOT IN`, CASE, and IF inherit the membership semantics.
7. `ARRAY_CONTAINS`, `CARDINALITY`, `ARRAY_LENGTH`, and `ARRAY_JOIN` now parse, route through analytics, and return typed rows.
8. ARRAY `!=`, range, `BETWEEN`, `LIKE`, and `REGEXP` use the approved any-element contract.
9. Twelve scalar string functions map element-wise over ARRAY values. `NULLIF` preserves null children, and `COALESCE` supplies a singleton fallback for null arrays.
10. SQL now parses ARRAY literals, empty literals, subscripts, and `COALESCE` through the Calcite/DataFusion route.
11. DSL conversion ignores internal `_doc` and `ShardDocSortBuilder` ordering while retaining mapped sort fields.

Sorting by an ordinary scalar field no longer fails schema conversion. The converter filters internal `_doc` and shard-document ordering before resolving mapped fields. The SQL route still returns zero rows for the O4 scalar sort probe, while PPL returns all six rows correctly.

## Environment

- OpenSearch repository: `/local/home/bansvaru/codebase/oss-dev/OpenSearch-array-semantics`
- SQL frontend repository: `/local/home/bansvaru/codebase/oss-dev/sql` at base `96399c590b41`
- Branch: `multi-value-integration-testing`
- Base commit: `83a2244a89c`
- Server: Gradle `run`, OpenSearch `3.9.0-SNAPSHOT`
- Plugins: arrow-base, arrow-flight-rpc, analytics-engine, composite-engine, parquet-data-format, analytics-backend-datafusion, analytics-backend-lucene, dsl-query-executor, opensearch-job-scheduler, opensearch-sql
- Cluster settings: composite data format enabled; scoped page index enabled
- Native library: rebuilt from the isolated worktree with `--rerun-tasks`; SHA-256 `05185c4b4014a4ac1df5eab1ad111a1c8c8b13f2ec2146a2db36fbab03bf63c0`

## Fixture

Index: `mv_array_runtime_final4`, one shard, no replicas, composite primary Parquet + secondary Lucene.

```json
{"id":"d3","tags":"prod","score":30}
```

The first document was flushed to create a scalar `Utf8` generation. These documents were then indexed, triggering `tags.multi_value=true`:

```json
{"id":"d1","tags":["prod","blue"],"score":10}
{"id":"d2","tags":["blue"],"score":20}
{"id":"d4","tags":[],"score":40}
{"id":"d5","tags":null,"score":50}
{"id":"d6","tags":[null],"score":60}
```

Observed mapping:

```json
{"type":"keyword","multi_value":true}
```

Composite append-only behavior: `PUT /mv_array_runtime_final4/_doc/{custom-id}` is rejected with `Operation [INDEX] is not allowed`. The fixture used `POST /_doc` auto IDs and retained `id` in the document body.

## Implemented fixes

| Fix | Source | Regression evidence |
|---|---|---|
| Sort before source projection | `SearchSourceConverter` | `SearchSourceConverterTests.testSortFieldMayBeExcludedFromSourceProjection` |
| `MIN(ARRAY)` / `MAX(ARRAY)` hidden scalar reductions | `MultiValueRelRewriter`, `list_min.rs`, Substrait mappings | `DataFusionFragmentConvertorTests.testListMinMaxAggregatesUseHiddenScalarReductions`; Rust suite |
| Exact `COUNT(DISTINCT ARRAY)` | Route to existing list-aware `os_count_distinct` | `DataFusionFragmentConvertorTests.testListApproxCountDistinctUsesListAwareExactUdaf` |
| Aggregate alias output alignment | `DefaultPlanExecutor.orderedColumns` guarded ordinal alias fallback | `DefaultPlanExecutorTests.testBatchesToRowsResolvesAggregateAliasByOrdinal`; missing-name invariant still passes |
| Explicit `mvexpand` | `EngineCapability.MULTI_VALUE_EXPAND`, marked Correlate/Uncollect nodes and rules, cluster-copy support | `MultiValueKeywordIT.testExplicitMvexpandHonorsPerDocumentLimit` |
| ARRAY `UNION` FFI safety | Stream-schema alignment plus nested variable-width re-materialization before Arrow C export | Rust normalization tests plus fresh-server DISTINCT/ALL probes |
| ARRAY equality and membership | `CalciteRexNodeVisitor` lowers ARRAY/scalar `=` and ARRAY `IN` to typed `ARRAY_CONTAINS` | Calcite PPL tests plus SQL endpoint W1/W1R/W3/W4/E1/E2 |
| Customer-visible ARRAY functions | SQL grammar, `PPLFuncImpTable`, legacy schema typing, analytics `ScalarFunction`, DataFusion extension mapping | SQL parser, function registry, converter tests, and endpoint X5/X6/Y11/Y12 |
| Any-element predicates | `CalciteRexNodeVisitor`, `array_any_compare`, `array_any_between` | W2, W5, W6, W7, and W11 fresh-server results |
| Element-wise expressions | `array_map_string`, `array_map_integer`, `array_nullif`, `array_coalesce` | 12 SQL functions, NULLIF, COALESCE, and PPL lower/replace |
| ARRAY syntax | Both SQL grammar copies and `AstExpressionBuilder` | literals, empty literals, field subscripts, and literal subscripts |
| Internal sort handling | `SortConverter` | complete `SortConverterTests` and `SearchSourceConverterTests` classes |
| LIST child null preservation | `ArrowValues.toJavaValue` offset-based recursive conversion | `ArrowValuesTests.testListOfStringPreservesNullElements` and runtime NULLIF |

Validation:

- DataFusion Rust: 1,336 passed, 0 failed, 1 ignored.
- Complete affected Java test classes: passed.
- Spotless and `git diff --check`: passed.
- Fresh isolated-worktree native build: passed in 12m50s; exact release library hash recorded above.
- `MultiValueKeywordIT`: 6 tests, 2 expected skips, 0 failures/errors.
- `MultiValueKeywordNoMergeIT`: 1 test, 0 skips/failures/errors.
- SQL core, PPL Calcite, and SQL grammar tests: passed.
- Analytics ScalarFunction and complete DataFusion converter tests: passed.
- Fresh-server fixture: 6 documents; mapping promoted to LIST.
- Focused approved-semantics matrix: 30 passed, 0 failed in `/tmp/array-semantics-runtime-results.json`.
- Full compatibility matrix: 66 meaningful, 8 explicit failures, and 4 silent or malformed HTTP-200 responses in `/tmp/mv-final-query-results-array-semantics.json`.

## SQL sweep: projections and reads

| ID | SQL | Server result |
|---|---|---|
| P1 | `SELECT id, tags FROM mv_sql_sweep` | PASS, 6 rows, schema `id:string, tags:array`; d3 returns `["prod"]`, d1 returns `["prod","blue"]` |
| P2 | `SELECT tags FROM mv_sql_sweep` | PASS, 6 rows, typed ARRAY projection |
| P3 | `SELECT * FROM mv_sql_sweep` | PASS, 6 rows, `id:string, score:integer, tags:array` |
| P4 | `SELECT DISTINCT tags FROM mv_sql_sweep` | PASS with metadata caveat: values `blue`, null, `prod` are scalar-expanded while response labels `tags:array` |

## SQL sweep: predicates

| ID | SQL | Server result |
|---|---|---|
| W1 | `WHERE tags = 'prod'` | FIXED: d1 and scalar-generation d3 |
| W2 | `WHERE tags != 'prod'` | FIXED: d2 and empty-list d4; null rows remain SQL UNKNOWN |
| W3 | `WHERE tags IN ('prod','error')` | FIXED: d1 and d3 |
| W4 | `WHERE tags NOT IN ('prod')` | FIXED: d2 and empty-list d4; null rows remain SQL UNKNOWN |
| W5 | `WHERE tags > 'blue'` | FIXED: d1 and scalar-generation d3 |
| W6 | `WHERE tags BETWEEN 'a' AND 'q'` | FIXED: d1, d2, and d3 |
| W7 | `WHERE tags LIKE 'pro%'` | FIXED: d1 and d3 |
| W8 | `WHERE tags IS NULL` | PASS, returns d5 (null) and d6 (`[null]`) |
| W9 | `WHERE tags IS NOT NULL` | PASS, 4 rows; empty list d4 is non-null |
| W10 | `WHERE id = 'd1'` projecting tags | PASS, returns `d1, ["prod","blue"]` |
| W11 | `WHERE tags REGEXP 'pro.*'` | FIXED: d1 and d3 |
| X8 | `WHERE tags = ARRAY['prod','blue']` | FIXED: exact whole-array equality returns d1 |

## SQL sweep: scalar functions and expressions

| ID | SQL | Server result |
|---|---|---|
| F1 | `UPPER(tags)` | FIXED: element-wise ARRAY output, including `["PROD","BLUE"]` for d1 |
| F2 | `LENGTH(tags)` | FIXED: `[4,4]`, `[4]`, `[4]`, `[]`, null, null |
| F3 | `CONCAT(tags, '-x')` | PASS via whole-array stringification; d1 becomes `"[prod, blue]-x"` |
| F4 | `SUBSTRING(tags,1,3)` | FIXED: d1 returns `["pro","blu"]` |
| F5 | `TRIM(tags)` | FIXED: element-wise ARRAY output with null/empty preservation |
| F6 | `CAST(tags AS STRING)` | PASS; d1 becomes `"[prod, blue]"` |
| E1 | `CASE WHEN tags='prod' ...` | FIXED: y for d1/d3, n for d2/d4/d5/d6 |
| E2 | `IF(tags='prod',...)` | FIXED: same membership result as CASE |
| X3 | `COALESCE(tags,'none')` | FIXED: preserves non-null arrays; d5 and d6 return `["none"]` |
| X4 | `NULLIF(tags,'prod')` | FIXED: matching elements become null without changing list length |
| X5 | `CARDINALITY(tags)` | FIXED: d1=2, d2=1, d3=1, d4=0, d5/d6=null |
| X6 | `ARRAY_LENGTH(tags)` | FIXED: same typed integer results as CARDINALITY |
| X7 | `tags[1]` | FIXED: 1-based first element; empty/null arrays return null |
| X11 | `tags + 1` | HTTP 400: ADD got ARRAY/INTEGER |

Validated interim workaround:

```sql
SELECT id FROM mv_sql_sweep WHERE CAST(tags AS STRING) LIKE '%prod%'
```

Returns d3 and d1. This is full-scan substring matching over rendered arrays, not element-safe membership.

## SQL sweep: aggregations

| ID | SQL | Server result after fixes |
|---|---|---|
| A1 | `COUNT(*)` | PASS: 6 |
| A2 | `COUNT(tags)` | PASS: 4 (non-null LIST rows; d5 and d6 excluded, empty d4 included) |
| A3 | `COUNT(DISTINCT tags)` | FIXED: PASS, exact distinct elements = 2 (`blue`, `prod`) |
| A4 | `MIN(tags)` | FIXED: PASS, value `blue`; response type still mislabeled `array` |
| A5 | `MAX(tags)` | FIXED: PASS, value `prod`; response type still mislabeled `array` |
| A6 | `GROUP BY tags` | PASS: `blue=2`, `null=2`, `prod=2`; group keys scalar-expanded but response type says `array` |
| A7 | `GROUP BY tags HAVING c > 1` | FIXED: PASS, same three groups, alias `c` preserved |
| A8 | `GROUP BY id, COUNT(tags)` | PASS, six groups; d5/d6 count 0 |
| X1 | `SUM(tags)` | HTTP 400: SUM supports numeric types, not ARRAY |
| X2 | `AVG(tags)` | HTTP 400: AVG supports numeric types, not ARRAY |
| X12 | `GROUP BY tags ORDER BY c DESC` | PASS, three groups |

PPL aggregate coverage:

| Query | Server result |
|---|---|
| `stats min(tags) as mn, max(tags) as mx` | FIXED: `blue`, `prod`; response schema still says array |
| `stats distinct_count(tags) as dc` | FIXED: 2 |
| `stats count() as c by tags | where c > 1` | FIXED: three rows; alias available to filter |
| `top 10 tags` | PASS: `blue=2`, `prod=2`, `null=2` |
| `rare 10 tags` | PASS: same counts |
| `stats list(tags)` | HTTP 400: sql frontend rejects ARRAY input |
| `stats values(tags)` | HTTP 400: sql frontend rejects ARRAY input |

## SQL sweep: sorting, windows, set operations

| ID | SQL | Server result |
|---|---|---|
| O1 | `ORDER BY tags ASC` | PASS, 6 rows; missing region first, then fixed-MIN order |
| O2 | `ORDER BY tags DESC` | PASS, 6 rows |
| O3 | `ORDER BY tags LIMIT 2` | PASS, 2 rows |
| O4 | `ORDER BY score DESC` | PARTIAL FIX: schema-conversion error removed, but SQL legacy route returns HTTP 200 with zero rows |
| PPL | `sort - score | fields id,score,tags` | PASS, 6 rows: d6(60), d5(50), d4(40), d3(30), d2(20), d1(10) |
| X9 | `ROW_NUMBER() OVER (PARTITION BY tags ...)` | PASS, 6 rows with whole-array partition semantics |
| X10 | `COUNT(*) OVER (PARTITION BY tags)` | PASS, 6 rows; whole-array partition semantics |
| S1 | scalar subquery `IN` | BROKEN: HTTP 200 with zero rows |
| S2 | self JOIN on `a.tags=b.tags` | PASS, 4 rows with whole-array equality semantics; not any-element membership |
| S3 | `UNION` of ARRAY projections | FIXED: DISTINCT returns five whole-array values (`["prod","blue"]`, `[]`, null, `["prod"]`, `["blue"]`); UNION ALL returns 12 rows from two six-row branches |

## PPL expansion and tolerance

| Query / setting | Server result |
|---|---|
| `where tags='prod'` | FIXED: d1 and d3 through ARRAY_CONTAINS lowering |
| `eval x=lower(tags)` and `eval x=replace(tags,...)` | FIXED: element-wise ARRAY output for all six rows |
| `mvexpand tags` | FIXED: expands d1 to `blue`,`prod`; d2 to `blue`; scalar-generation d3 to `prod`; null rows remain null; empty list emits no row |
| `plugins.query.field_type_tolerance=true` | PASS: typed ARRAY projection and scalar-filter results unchanged |
| `plugins.query.field_type_tolerance=false` | PASS: identical results; tolerance does not coerce typed ARRAY output |

## Extended inventory sweep

This final pass executed every named function and grouping-adjacent operator from the Deliverable 8 inventory.

| ID | Query | Server result |
|---|---|---|
| Y1 | `LOWER(tags)` | FIXED: element-wise ARRAY output |
| Y2 | `LTRIM(tags)` | FIXED: element-wise ARRAY output |
| Y3 | `RTRIM(tags)` | FIXED: element-wise ARRAY output |
| Y4 | `REPLACE(tags,'p','x')` | FIXED: d1 returns `["xrod","blue"]` |
| Y5 | `REVERSE(tags)` | FIXED: d1 returns `["dorp","eulb"]` |
| Y6 | `SPLIT(tags,',')` | BROKEN: HTTP 200, incorrect `double` schema, zero rows |
| Y7 | `POSITION('p' IN tags)` | FIXED: element-wise integer ARRAY |
| Y8 | `LOCATE('p',tags)` | FIXED: element-wise integer ARRAY; optional start position also passes |
| Y9 | `CHAR_LENGTH(tags)` | BROKEN: HTTP 200 with no schema and zero rows |
| Y10 | `ASCII(tags)` | FIXED: d1 returns `[112,98]` |
| Y11 | `ARRAY_CONTAINS(tags,'prod')` | FIXED: true for d1/d3, false for d2/d4, null for d5/d6 |
| Y12 | `ARRAY_JOIN(tags,',')` | FIXED: `prod,blue`, `blue`, `prod`, empty string, null, null |
| Y13 | PPL `dedup tags` | PASS, 4 rows (whole-array dedup; null shapes collapse) |
| Y14 | PPL `eventstats count() by tags` | PASS, 6 rows, whole-array grouping semantics |
| Y15 | PPL `streamstats count() by tags` | PASS, 6 rows, whole-array grouping semantics |
| Y16 | PPL `parse tags ...` | HTTP 400: INTERNAL_PARSE expects STRING, got ARRAY |
| Y17 | PPL `rex field=tags ...` | HTTP 400: REX_EXTRACT expects STRING, got ARRAY |
| Y18 | PPL `grok tags ...` | HTTP 400: INTERNAL_GROK expects STRING, got ARRAY |
| Y19 | PPL `eval x=lower(tags)` | FIXED: element-wise ARRAY output |
| Y20 | PPL `eval x=replace(tags,...)` | FIXED: element-wise ARRAY output |

Across 78 query forms, 66 return meaningful results, 8 fail explicitly, and 4 return HTTP 200 with silent empty or malformed results. `UNION ALL` returns 12 rows, and four additional F13 runs confirm `field_type_tolerance` on/off invariance. The final full sweep is in `/tmp/mv-final-query-results-array-semantics.json`; the focused 30-case contract matrix is in `/tmp/array-semantics-runtime-results.json`.

## Remaining ownership and decisions

### opensearch-sql frontend repository

- `stats list(tags)` and `stats values(tags)` still reject ARRAY input explicitly.
- `SPLIT(tags, ',')` and `CHAR_LENGTH(tags)` still return silent or malformed HTTP-200 responses.
- The scalar `ORDER BY score` probe and scalar subquery `IN` probe still return silent HTTP-200 results.
- `parse`, `rex`, and `grok` reject ARRAY input by design. Customers must use explicit `mvexpand` before these correlation-sensitive extraction operators.

### Core analytics repository

- Distributed LIST GROUP BY partial/final stitching remains a tracked gap. Java POJO decoding loses `ExtensionSingleRel` output schema. A proto-stitching experiment progressed past that failure but exposed incorrect stage input binding (`No table named <source>`), so the incomplete change was reverted.
- Response schema metadata still labels expanded GROUP BY keys and MIN/MAX outputs as ARRAY while values are scalar.

### Defined semantics

- `!=`, range, `BETWEEN`, `LIKE`, and `REGEXP` use any-element matching. Null lists remain SQL UNKNOWN, empty lists match negated membership, and matching stops after the first non-null match.
- Supported scalar string functions map element-wise, preserve list order and null children, and return typed ARRAY values.
- `NULLIF` replaces matching elements with null. `COALESCE` preserves non-null arrays and converts a scalar fallback into a singleton array for null inputs.
- `SUM`, `AVG`, and arithmetic on ARRAY<STRING> remain valid type errors, as they are for scalar STRING. Numeric ARRAY aggregation has no defined contract.
- JOIN keys retain whole-array equality semantics.

## Conclusion

The approved ARRAY contract now passes 30 of 30 focused fresh-server cases. The full 78-query matrix returns 66 meaningful results, 8 explicit failures, and 4 silent or malformed HTTP-200 responses. Predicate semantics, 12 element-wise functions, NULLIF, COALESCE, ARRAY literals, empty literals, subscripts, and PPL lower/replace are implemented. Remaining work is limited to list/values aggregation input, SPLIT and CHAR_LENGTH, two silent SQL plan shapes, distributed GROUP BY stage binding, and response-schema labels.
