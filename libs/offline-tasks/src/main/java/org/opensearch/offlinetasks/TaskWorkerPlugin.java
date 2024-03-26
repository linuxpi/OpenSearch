/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offlinetasks;

/**
 * sample
 */
public interface TaskWorkerPlugin {

    /**
     * sample
     */
    TaskWorker registerTaskWorker();

    /**
     * sample
     */
    TaskType getType();
}
