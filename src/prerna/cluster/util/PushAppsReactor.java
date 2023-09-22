package prerna.cluster.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.cluster.util.clients.ICloudClient;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PushAppsReactor extends AbstractReactor{

	public PushAppsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DRY_RUN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String dryRunString = this.keyValue.get(ReactorKeysEnum.DRY_RUN.getKey());

		boolean dryRun = true;
		if (dryRunString != null && !dryRunString.isEmpty() && dryRunString.equalsIgnoreCase("false")) {
			dryRun = false;
		}
		
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
		if(!isAdmin) {
			throw new IllegalArgumentException("User must be an admin for this operation!");
		}
		
		Map<String, Object> pushedApps = new HashMap<String, Object>();
		pushedApps.put("dryRun", dryRun);
		
		
		//Get all Engines
		List<String> appIds = SecurityEngineUtils.getAllEngineIds();

		try {
			ICloudClient cc = CentralCloudStorage.getInstance();
			List<String> allContainers = cc.listAllBlobContainers();
			List<String> cleanedNames = new ArrayList<String>();
			for (String container : allContainers) {
				if(container.contains("smss")){
					continue;
				}
				String cleanedContainerName = container.replaceAll("-smss", "").replaceAll("/", "");
				cleanedNames.add(cleanedContainerName);
			}
			
			for(String appId : appIds){
				if(!cleanedNames.contains(appId)){
					pushedApps.put(appId, "pushed");
					if(!dryRun){
						cc.pushEngine(appId);
					}
				} else{
					pushedApps.put(appId, "Already exists");
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(pushedApps, PixelDataType.MAP, PixelOperationType.CLEANUP_APPS);
	}

}
