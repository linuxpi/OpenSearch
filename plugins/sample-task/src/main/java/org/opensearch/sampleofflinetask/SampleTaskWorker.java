/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sampleofflinetask;

import org.opensearch.offlinetasks.Task;
import org.opensearch.offlinetasks.TaskWorker;

/**
 * sample
 */
public class SampleTaskWorker implements TaskWorker {

    /**
     * sample
     */
    public SampleTaskWorker() {}

    /**
     * sample
     * @param task sample
     */
    @Override
    public void doWork(Task task) {
        System.out.println(task.toString());
    }
}
