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

		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
        // parse the user inputs into a 'keyValue' map
        organizeKeys();


		String hostname = System.getenv("hostname");

        Map<String, Object> systemInfoMap = AdminGetSystemInfoReactor.getSystemInfoDetailsMap(hostname);
        return new NounMetadata(systemInfoMap, PixelDataType.MAP);
	}


    
    public static Map<String, Object> getSystemInfoDetailsMap(String hostname){


        Map<String, Object> systemInfoDetailsmap =  new HashMap<>();
        systemInfoDetailsmap.put("isClustered", Boolean.valueOf(ClusterUtil.IS_CLUSTER));
        systemInfoDetailsmap.put("STORAGE_PROVIDER", ClusterUtil.STORAGE_PROVIDER);
        systemInfoDetailsmap.put("REMOTE_RSERVE",ClusterUtil.REMOTE_RSERVE);
        systemInfoDetailsmap.put("areLoadEnginesLocally", Boolean.valueOf(ClusterUtil.LOAD_ENGINES_LOCALLY));
        systemInfoDetailsmap.put("hostname", hostname);
        systemInfoDetailsmap.put("IMAGES_FOLDER_PATH", ClusterUtil.IMAGES_FOLDER_PATH);
        systemInfoDetailsmap.put("IS_CLUSTERED_SCHEDULER", Boolean.valueOf(ClusterUtil.IS_CLUSTERED_SCHEDULER));
        return systemInfoDetailsmap;
        
    }
}
