/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared index and response helpers for composed multi-value keyword integration tests. */
abstract class MultiValueKeywordTestCase extends AnalyticsRestTestCase {

    protected void createMultiValueIndex(String index, int shards, boolean adaptive, boolean indexSorted) throws IOException {
        deleteIndexIfExists(index);
        StringBuilder settings = new StringBuilder()
            .append("\"number_of_shards\":").append(shards).append(',')
            .append("\"number_of_replicas\":0,")
            .append("\"index.refresh_interval\":\"-1\",")
            .append("\"index.pluggable.dataformat.enabled\":true,")
            .append("\"index.pluggable.dataformat\":\"composite\",")
            .append("\"index.composite.primary_data_format\":\"parquet\",")
            .append("\"index.composite.secondary_data_formats\":\"lucene\"");
        if (indexSorted) {
            settings.append(',')
                .append("\"index.sort.field\":\"tags\",")
                .append("\"index.sort.order\":\"asc\",")
                .append("\"index.sort.missing\":\"_first\"");
        }
        String multiValueParameter = adaptive ? "" : ",\"multi_value\":true";
        String body = "{\"settings\":{" + settings + "},\"mappings\":{\"properties\":{" +
            "\"id\":{\"type\":\"keyword\"}," +
            "\"tags\":{\"type\":\"keyword\"" + multiValueParameter + "}," +
            "\"colors\":{\"type\":\"keyword\",\"multi_value\":true}" +
            "}}}";
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(body);
        assertEquals(true, assertOkAndParse(client().performRequest(create), "create " + index).get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    protected String indexDocument(String index, String json, boolean refresh) throws IOException {
        Request request = new Request("POST", "/" + index + "/_doc");
        request.addParameter("refresh", Boolean.toString(refresh));
        request.setJsonEntity(json);
        Response raw = client().performRequest(request);
        int status = raw.getStatusLine().getStatusCode();
        assertTrue("index into " + index + ": expected HTTP 200/201 but got " + status, status == 200 || status == 201);
        Map<String, Object> response = entityAsMap(raw);
        return String.valueOf(response.get("_id"));
    }

    protected void bulkAndRefresh(String index, List<String> documents) throws IOException {
        StringBuilder body = new StringBuilder();
        for (String document : documents) {
            body.append("{\"index\":{}}\n").append(document).append('\n');
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        request.setJsonEntity(body.toString());
        assertEquals(false, assertOkAndParse(client().performRequest(request), "bulk " + index).get("errors"));
    }

    protected void flush(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_flush");
        request.addParameter("force", "true");
        assertOkAndParse(client().performRequest(request), "flush " + index);
    }

    @SuppressWarnings("unchecked")
    protected void forceMerge(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        request.addParameter("flush", "true");
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "force merge " + index);
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertEquals(0, ((Number) shards.get("failed")).intValue());
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getSource(String index, String documentId) throws IOException {
        Map<String, Object> response = assertOkAndParse(
            client().performRequest(new Request("GET", "/" + index + "/_doc/" + documentId)),
            "get " + documentId
        );
        assertEquals(true, response.get("found"));
        return (Map<String, Object>) response.get("_source");
    }

    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> pplRows(String query) throws IOException {
        Map<String, Object> response = executePpl(query);
        List<String> columns = extractColumnNames(response);
        List<List<Object>> data = (List<List<Object>>) response.get("datarows");
        List<Map<String, Object>> rows = new ArrayList<>(data.size());
        for (List<Object> values : data) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int index = 0; index < columns.size(); index++) {
                row.put(columns.get(index), values.get(index));
            }
            rows.add(row);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    protected static List<String> strings(Object value) {
        assertTrue("expected LIST value but got " + value, value instanceof List<?>);
        return ((List<Object>) value).stream().map(String::valueOf).toList();
    }

    protected static long longValue(Object value) {
        return ((Number) value).longValue();
    }

    private void deleteIndexIfExists(String index) throws IOException {
        Request request = new Request("DELETE", "/" + index);
        request.addParameter("ignore_unavailable", "true");
        Response response = client().performRequest(request);
        assertTrue(response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 404);
    }
}
