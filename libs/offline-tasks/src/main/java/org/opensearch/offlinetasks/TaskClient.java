/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offlinetasks;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * sample
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TaskClient {

    /**
     * sample
     * @param task
     */
    void submitTask(Task task);

    /**
     * sample
     */
    List<Task> getUnassignedTasks();

}
