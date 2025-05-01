package prerna.cluster.util;

import prerna.engine.api.RemoteModelStateEnum;

/**
 * Represents information about a model deployed in the remote infrastructure.
 */
public class RemoteModelInfo {
    private final String id;
    private final String name;
    private final RemoteModelStateEnum state;

    public RemoteModelInfo(String id, String name, RemoteModelStateEnum state) {
        this.id = id;
        this.name = name;
        this.state = state;
    }

    // Getters
    public String getId() { return id; }
    public String getName() { return name; }
    public RemoteModelStateEnum getState() { return state; }
}