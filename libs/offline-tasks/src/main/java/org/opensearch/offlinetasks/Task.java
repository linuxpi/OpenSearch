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
public class Task {
    /**
     * sample
     */
    private String id;

    /**
     * sample
     */
    private TaskType type;

    /**
     * sample
     * @param id sample
     * @param type sample
     */
    public Task(String id, TaskType type) {
        this.id = id;
        this.type = type;
    }

    /**
     * sample
     */
    public TaskType getType() {
        return type;
    }

    /**
     * sample
     */
    @Override
    public String toString() {
        return this.id;
    }
}
