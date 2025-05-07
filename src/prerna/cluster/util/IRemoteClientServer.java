package prerna.cluster.util;

import java.util.List;
import java.util.Map;

import prerna.engine.api.RemoteModelStateEnum;

/**
 * Interface to abstract the remote client server functionality.
 * This allows both ZooKeeper-based and REST-based implementations.
 */
public interface IRemoteClientServer {
    
    /**
     * Gets the current state of a model
     * @param modelId The model ID
     * @return The current state of the model
     */
    RemoteModelStateEnum getModelState(String modelId);
    
    /**
     * Gets the cluster IP address for the specified model
     * @param modelId The model ID
     * @return The cluster IP address, or null if not found
     */
    String getModelClusterIp(String modelId);
    
    /**
     * Gets the cluster IP address for the KMS server
     * @return The cluster IP address, or null if not found
     */
    String getModelScalerIp();
    
    /**
     * Gets the friendly name of the model
     * @param modelId The model ID
     * @return The model name, or null if not found
     */
    String getModelName(String modelId);
    
    /**
     * Checks if the model is currently warming up
     * @param modelId The model ID
     * @return true if the model is warming, false otherwise
     */
    boolean isModelWarming(String modelId);
    
    /**
     * Checks if the model is currently active
     * @param modelId The model ID
     * @return true if the model is active, false otherwise
     */
    boolean isModelActive(String modelId);
    
    /**
     * Waits for a model to become active
     * @param modelId The model ID
     * @param timeoutMs Maximum time to wait in milliseconds
     * @return true if the model became active within the timeout, false otherwise
     */
    boolean waitForModelActive(String modelId, long timeoutMs);
    
    /**
     * Waits for a model to reach a specific state
     * @param modelId The model ID
     * @param desiredState The state to wait for
     * @param timeoutMs Maximum time to wait in milliseconds
     * @return true if the model reached the desired state within the timeout, false otherwise
     */
    boolean waitForState(String modelId, RemoteModelStateEnum desiredState, long timeoutMs);
    
    /**
     * Gets a list of active models
     * @return List of RemoteModelInfo objects for active models
     */
    List<RemoteModelInfo> getActiveModels();
    
    /**
     * Gets a list of warming models
     * @return List of RemoteModelInfo objects for warming models
     */
    List<RemoteModelInfo> getWarmingModels();
}