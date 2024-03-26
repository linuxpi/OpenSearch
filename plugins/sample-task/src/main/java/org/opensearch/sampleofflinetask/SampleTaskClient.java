/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sampleofflinetask;

import org.opensearch.offlinetasks.Task;
import org.opensearch.offlinetasks.TaskClient;
import org.opensearch.offlinetasks.TaskType;

import java.util.ArrayList;
import java.util.List;

/**
 * sample
 */
public class SampleTaskClient implements TaskClient {

    /**
     * sample
     */
    public SampleTaskClient() {}

    /**
     * sample
     * @param task sample
     */
    @Override
    public void submitTask(Task task) {
        // default implementation
    }

    /**
     * sample
     */
    @Override
    public List<Task> getUnassignedTasks() {
        return new ArrayList<>(){
            {
                add(new Task("sample-task", TaskType.SAMPLE));
                add(new Task("merge-task", TaskType.MERGE));
            }
        };
    }

}
