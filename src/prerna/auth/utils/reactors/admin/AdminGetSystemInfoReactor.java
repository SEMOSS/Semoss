package prerna.auth.utils.reactors.admin;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetSystemInfoReactor extends AbstractReactor {

    @Override
    public NounMetadata execute() {
        User user = this.insight.getUser();
        SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
        if (adminUtils == null) {
            throw new IllegalArgumentException("User must be an admin to perform this function");
        }

        String hostname;
        try {
            hostname = System.getenv("hostname");
            if (hostname == null || hostname.isEmpty()) {
                hostname = java.net.InetAddress.getLocalHost().getHostName();
            }
        } catch (Exception e) {
            hostname = "unknown-host";
        }

        String ipaddress;
        try {
            ipaddress = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ipaddress = "unknown-ipaddress";
        }

        Map<String, Object> systemInfoDetailsmap = new HashMap<>();
        systemInfoDetailsmap.put("hostname", hostname);
        systemInfoDetailsmap.put("ipaddress", ipaddress);
        systemInfoDetailsmap.put("isCluster", ClusterUtil.IS_CLUSTER);
        systemInfoDetailsmap.put("storageProvider", ClusterUtil.STORAGE_PROVIDER);
        systemInfoDetailsmap.put("isClusterScheduler", ClusterUtil.IS_CLUSTERED_SCHEDULER);
        systemInfoDetailsmap.put("isClusterZK", ClusterUtil.IS_CLUSTER_ZK);
        return new NounMetadata(systemInfoDetailsmap, PixelDataType.MAP);
    }
    
    @Override
    public String getReactorDescription() {
    	return """
	    			Admin only reactor returning a map with properties about the instance including: 
	    			hostname, ipaddress, isCluster, storageProvider, isClusterScheduler, isClusterZK
    			""";
    }
 
}
