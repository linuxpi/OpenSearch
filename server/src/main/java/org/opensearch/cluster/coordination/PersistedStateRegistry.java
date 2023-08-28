/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.coordination.CoordinationState.PersistedState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class which encapsulates the PersistedStates
 *
 * @opensearch.internal
 */
public class PersistedStateRegistry {

    private static final PersistedStateRegistry INSTANCE = new PersistedStateRegistry();

    private PersistedStateRegistry() {}

    /**
     * Distinct Types PersistedState which can be present on a node
     */
    public enum PersistedStateType {
        LOCAL,
        REMOTE
    }

    private final Map<PersistedStateType, PersistedState> persistedStates = new ConcurrentHashMap<>();

    public static void addPersistedState(PersistedStateType persistedStateType, PersistedState persistedState) {
        PersistedState existingState = INSTANCE.persistedStates.putIfAbsent(persistedStateType, persistedState);
        assert existingState == null : "should only be set once, but already have " + existingState;
    }

    public static PersistedState getPersistedState(PersistedStateType persistedStateType) {
        return INSTANCE.persistedStates.get(persistedStateType);
    }

}
