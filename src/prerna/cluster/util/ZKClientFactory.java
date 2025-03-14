package prerna.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Factory class to get the appropriate ZooKeeper client implementation
 * based on the environment configuration.
 */
public class ZKClientFactory {
    
    private static final Logger classLogger = LogManager.getLogger(ZKClientFactory.class);
    
    /**
     * Gets the appropriate ZooKeeper client implementation based on environment variables.
     * 
     * @return An implementation of IRemoteClientServer
     */
    public static IRemoteClientServer getZKClient() {
        String zkIngress = System.getenv("ZK_INGRESS");
        
        if (zkIngress != null && !zkIngress.isEmpty()) {
            classLogger.info("ZK_INGRESS found, using REST proxy connection for ZooKeeper");
            
            // When using ZK REST proxy, KMS_INGRESS is required
            String kmsIngress = System.getenv("KMS_INGRESS");
            if (kmsIngress == null || kmsIngress.isEmpty()) {
                classLogger.warn("KMS_INGRESS environment variable is not set but required for ZK REST Proxy mode");
            }
            
            return RemoteClientServerZKRESTProxy.getInstance();
        } else {
            classLogger.info("Using direct ZooKeeper connection (cluster mode)");
            return RemoteClientServerZK.getInstance();
        }
    }
}