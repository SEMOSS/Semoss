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
        // parse the user inputs into a 'keyValue' map
        organizeKeys();

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
        systemInfoDetailsmap.put("isClusteredZK", ClusterUtil.IS_CLUSTER_ZK);
        systemInfoDetailsmap.put("STORAGE_PROVIDER", ClusterUtil.STORAGE_PROVIDER);
        systemInfoDetailsmap.put("hostname", hostname);
        systemInfoDetailsmap.put("IPAddress", ipaddress);
        systemInfoDetailsmap.put("IS_CLUSTERED_SCHEDULER", ClusterUtil.IS_CLUSTERED_SCHEDULER);

        return new NounMetadata(systemInfoDetailsmap, PixelDataType.MAP);
    }
 
}
