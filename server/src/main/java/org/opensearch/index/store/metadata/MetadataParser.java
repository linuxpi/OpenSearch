/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.metadata;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Map;

public interface MetadataParser<T> {
    T readContent(IndexInput indexInput) throws IOException;

    void writeContent(IndexOutput indexOutput, T content) throws IOException;

    /**
     * This method is to be removed in future and above method is supposed to be used
     * @param indexOutput
     * @param content
     * @throws IOException
     */
    @Deprecated
    void writeContent(IndexOutput indexOutput, Map<String, String> content) throws IOException;
}
