/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.coordination.CoordinationState.PersistedState;

import java.util.HashMap;
import java.util.Map;

/**
 * A class which encapsulates the PersistedStates
 *
 * @opensearch.internal
 */
public class PersistentStateRegistry {

    private static final PersistentStateRegistry INSTANCE = new PersistentStateRegistry();

    private PersistentStateRegistry() {}

    /**
     * Distinct Types PersistedState which can be present on a node
     */
    public enum PersistedStateType {
        LOCAL,
        REMOTE
    }

    private final Map<PersistedStateType, PersistedState> persistentStates = new HashMap<>();

    public static void addPersistedState(PersistedStateType persistedStateType, PersistedState persistedState) {
        PersistedState existingState = INSTANCE.persistentStates.putIfAbsent(persistedStateType, persistedState);
        assert existingState == null : "should only be set once, but already have " + existingState;
    }

    public static PersistedState getPersistedState(PersistedStateType persistedStateType) {
        return INSTANCE.persistentStates.get(persistedStateType);
    }

}
