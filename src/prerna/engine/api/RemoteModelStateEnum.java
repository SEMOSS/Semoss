package prerna.engine.api;

/**
 * Enumeration representing the operational states of remote AI/ML models.
 * 
 * <p>This enum defines the possible states that a remotely hosted model can be in,
 * particularly relevant for cloud-hosted models, containerized models, or models
 * served through platforms like KServe, SageMaker, or other model serving infrastructure.
 * Understanding model state is crucial for request routing, scaling decisions, and
 * user experience optimization.</p>
 * 
 * <p>Model state transitions typically follow this pattern:</p>
 * <pre>
 * COLD → WARMING → ACTIVE
 *   ↓       ↓        ↓
 * FAILED ← FAILED ← FAILED
 * </pre>
 * 
 * <p>State implications:</p>
 * <ul>
 *   <li><strong>COLD:</strong> Model is not loaded; requests will trigger cold start</li>
 *   <li><strong>WARMING:</strong> Model is loading; requests may queue or fail</li>
 *   <li><strong>ACTIVE:</strong> Model is ready; requests can be processed immediately</li>
 *   <li><strong>FAILED:</strong> Model failed to load; requests will fail</li>
 *   <li><strong>UNKNOWN:</strong> State cannot be determined; handle with caution</li>
 * </ul>
 * 
 * @see {@link IModelEngine} for model engine operations
 * @see {@link ModelTypeEnum#REMOTE} for remote model engine type
 * @author SEMOSS
 */
public enum RemoteModelStateEnum {
    /**
     * Model is not currently loaded in memory and will require initialization.
     * 
     * <p>In this state, the model container or service is not actively running
     * or the model weights are not loaded in memory. First requests will trigger
     * a "cold start" which may take significant time to complete.</p>
     */
    COLD,
    
    /**
     * Model is currently being loaded and initialized.
     * 
     * <p>The model is in the process of loading weights, initializing runtime
     * environments, or performing other startup operations. Requests during
     * this phase may be queued or rejected until the model becomes active.</p>
     */
    WARMING,
    
    /**
     * Model is fully loaded and ready to process requests.
     * 
     * <p>The model is operational and can process inference requests with
     * minimal latency. This is the optimal state for serving predictions
     * and handling user queries.</p>
     */
    ACTIVE,
    
    /**
     * Model failed to load or encountered a critical error.
     * 
     * <p>The model service is in an error state and cannot process requests.
     * This may be due to configuration issues, resource constraints, model
     * corruption, or other critical failures requiring intervention.</p>
     */
    FAILED,
    
    /**
     * Model state cannot be determined or is not available.
     * 
     * <p>The model's state is unknown, possibly due to network issues,
     * monitoring failures, or unsupported model serving platforms.
     * Requests should be handled cautiously with appropriate fallbacks.</p>
     */
    UNKNOWN
}
