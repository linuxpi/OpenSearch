/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Full-cluster coverage for composed multi-value keyword query behavior. */
public class MultiValueKeywordIT extends MultiValueKeywordTestCase {

    public void testForceMergedProjectionAndDerivedSourceGet() throws Exception {
        String index = "mv_kw_merged_get";
        createMultiValueIndex(index, 1, true, false);

        String scalarId = indexDocument(index, "{\"id\":\"before\",\"tags\":\"prod\"}", true);
        flush(index);
        String listId = indexDocument(index, "{\"id\":\"after\",\"tags\":[\"prod\",\"error\"]}", true);
        flush(index);
        forceMerge(index);

        List<Map<String, Object>> rows = pplRows("source=" + index + " | sort id | fields id, tags");
        assertEquals(List.of("prod", "error"), strings(rows.get(0).get("tags")));
        assertEquals(List.of("prod"), strings(rows.get(1).get("tags")));
        assertEquals(List.of("prod"), strings(getSource(index, scalarId).get("tags")));
        assertEquals(List.of("prod", "error"), strings(getSource(index, listId).get("tags")));
    }

    public void testQuerySortUsesMinimumAndPreservesVisibleLists() throws Exception {
        String index = "mv_kw_query_sort";
        createMultiValueIndex(index, 1, false, true);
        bulkAndRefresh(
            index,
            List.of(
                "{\"id\":\"d1\",\"tags\":[\"z\",\"delta\"]}",
                "{\"id\":\"d2\",\"tags\":[\"omega\",\"alpha\"]}",
                "{\"id\":\"d3\",\"tags\":[\"gamma\",\"beta\"]}",
                "{\"id\":\"missing\"}"
            )
        );

        List<Map<String, Object>> asc = pplRows("source=" + index + " | sort + tags | fields id, tags");
        assertEquals(List.of("missing", "d2", "d3", "d1"), asc.stream().map(row -> row.get("id")).toList());
        assertEquals(List.of("omega", "alpha"), strings(asc.get(1).get("tags")));
        assertEquals(List.of("gamma", "beta"), strings(asc.get(2).get("tags")));
        assertEquals(List.of("z", "delta"), strings(asc.get(3).get("tags")));

        List<Map<String, Object>> desc = pplRows("source=" + index + " | sort - tags | fields id, tags");
        assertEquals(List.of("d1", "d3", "d2", "missing"), desc.stream().map(row -> row.get("id")).toList());
        assertEquals(List.of("z", "delta"), strings(desc.get(0).get("tags")));
    }

    @AwaitsFix(
        bugUrl = "opensearch-sql frontend rejects LIST/VALUES over ARRAY before backend routing: "
            + "Aggregation function LIST expects scalar field type, but got ARRAY"
    )
    public void testListAndValuesAcrossShards() throws Exception {
        String index = "mv_kw_list_values";
        createMultiValueIndex(index, 2, false, false);
        bulkAndRefresh(
            index,
            List.of(
                "{\"id\":\"d1\",\"tags\":[\"b\",\"a\",\"b\"]}",
                "{\"id\":\"d2\",\"tags\":[\"c\",\"a\"]}",
                "{\"id\":\"d3\",\"tags\":[\"d\"]}"
            )
        );

        List<String> all = strings(pplRows("source=" + index + " | stats list(tags) as all").get(0).get("all"));
        Map<String, Long> frequencies = new HashMap<>();
        all.forEach(value -> frequencies.merge(value, 1L, Long::sum));
        assertEquals(Map.of("a", 2L, "b", 2L, "c", 1L, "d", 1L), frequencies);

        List<String> unique = strings(pplRows("source=" + index + " | stats values(tags) as unique").get(0).get("unique"));
        assertEquals(Set.of("a", "b", "c", "d"), new HashSet<>(unique));
        assertEquals(4, unique.size());
    }

    public void testListGroupByDeduplicatesPerDocumentAndFormsCartesianProduct() throws Exception {
        assertListGroupBySemantics("mv_kw_group_by", 1);
    }

    public void testDistributedListGroupByDeduplicatesPerDocumentAndFormsCartesianProduct() throws Exception {
        assertListGroupBySemantics("mv_kw_group_by_distributed", 2);
    }

    private void assertListGroupBySemantics(String index, int shards) throws Exception {
        createMultiValueIndex(index, shards, false, false);
        bulkAndRefresh(
            index,
            List.of(
                "{\"id\":\"d1\",\"tags\":[\"a\",\"a\",\"b\"],\"colors\":[\"red\",\"blue\"]}",
                "{\"id\":\"d2\",\"tags\":[\"a\"],\"colors\":[\"red\"]}"
            )
        );

        List<Map<String, Object>> oneKey = pplRows("source=" + index + " | stats count() as c by tags | sort tags");
        Map<String, Long> counts = new HashMap<>();
        oneKey.forEach(row -> counts.put(String.valueOf(row.get("tags")), longValue(row.get("c"))));
        assertEquals(Map.of("a", 2L, "b", 1L), counts);

        List<Map<String, Object>> twoKeys = pplRows("source=" + index + " | stats count() as c by tags, colors");
        Map<String, Long> pairs = new HashMap<>();
        twoKeys.forEach(
            row -> pairs.put(row.get("tags") + ":" + row.get("colors"), longValue(row.get("c")))
        );
        assertEquals(
            Map.of("a:red", 2L, "a:blue", 1L, "b:red", 1L, "b:blue", 1L),
            pairs
        );
    }

    public void testExplicitMvexpandHonorsPerDocumentLimit() throws Exception {
        String index = "mv_kw_expand";
        createMultiValueIndex(index, 1, false, false);
        bulkAndRefresh(
            index,
            List.of(
                "{\"id\":\"d1\",\"tags\":[\"b\",\"a\",\"c\"]}",
                "{\"id\":\"d2\",\"tags\":[\"x\"]}"
            )
        );

        List<Map<String, Object>> rows = pplRows(
            "source=" + index + " | mvexpand tags limit=2 | sort id, tags | fields id, tags"
        );
        assertEquals(3, rows.size());
        assertEquals(List.of("d1:a", "d1:b", "d2:x"), rows.stream().map(row -> row.get("id") + ":" + row.get("tags")).toList());
    }
}
