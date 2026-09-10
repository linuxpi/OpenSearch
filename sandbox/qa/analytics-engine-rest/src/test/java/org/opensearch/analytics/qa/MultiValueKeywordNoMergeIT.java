/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.List;
import java.util.Map;

/** Exercises mixed scalar/LIST parquet generations with background data-format merging disabled. */
public class MultiValueKeywordNoMergeIT extends MultiValueKeywordTestCase {

    public void testMixedUnmergedScalarAndListProjection() throws Exception {
        String index = "mv_kw_unmerged";
        createMultiValueIndex(index, 1, true, false);

        indexDocument(index, "{\"id\":\"before\",\"tags\":\"prod\"}", true);
        flush(index);
        indexDocument(index, "{\"id\":\"after\",\"tags\":[\"prod\",\"error\"]}", true);
        flush(index);

        List<Map<String, Object>> rows = pplRows("source=" + index + " | sort id | fields id, tags");
        assertEquals(2, rows.size());
        assertEquals("after", rows.get(0).get("id"));
        assertEquals(List.of("prod", "error"), strings(rows.get(0).get("tags")));
        assertEquals("before", rows.get(1).get("id"));
        assertEquals(List.of("prod"), strings(rows.get(1).get("tags")));
    }
}
