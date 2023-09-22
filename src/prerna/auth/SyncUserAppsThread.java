package prerna.auth;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.servlet.http.HttpSession;

import prerna.cluster.util.ClusterUtil;
import prerna.util.Constants;

public class SyncUserAppsThread implements Runnable {

	Collection<String> workspaceIds = null;
	Collection<String> assetIds = null;
	
	public SyncUserAppsThread(User user) {
		this.workspaceIds = user.getWorkspaceEngineMap().values();
		this.assetIds = user.getWorkspaceEngineMap().values();
	}
	
	public SyncUserAppsThread(Collection<String> workspaceIds, Collection<String> assetIds) {
		this.workspaceIds = workspaceIds;
		this.assetIds = assetIds;
	}
	
	public SyncUserAppsThread(String workspaceId, String assetId) {
		if(workspaceId != null) {
			this.workspaceIds = new ArrayList<String>();
			this.workspaceIds.add(workspaceId);
		}
		if(assetId != null) {
			this.assetIds = new ArrayList<String>();
			this.assetIds.add(assetId);
		}
	}
	
	@Override
	public void run() {
		execute(this.workspaceIds, this.assetIds);
	}
	
	public static void execute(HttpSession session) {
		if(ClusterUtil.IS_CLUSTER) {
			Collection<String> workspaceIds = null;
			Collection<String> assetIds = null;
			
			// now push all the values to be stored
			User user = (User) session.getAttribute(Constants.SESSION_USER);
			if(user != null) {
				workspaceIds = user.getWorkspaceEngineMap().values();
				assetIds = user.getAssetEngineMap().values();
			} else {
				// grab the maps from the session
				if(session.getAttribute(Constants.USER_WORKSPACE_IDS) != null) {
					workspaceIds = ((Map<AuthProvider, String>) session.getAttribute(Constants.USER_WORKSPACE_IDS)).values();
				}
				if(session.getAttribute(Constants.USER_ASSET_IDS) != null) {
					assetIds = ((Map<AuthProvider, String>) session.getAttribute(Constants.USER_ASSET_IDS)).values();
				}
			}
			
			execute(workspaceIds, assetIds);
		}
	}
	
	public static void execute(Collection<String> workspaceIds, Collection<String> assetIds) {
		// now push all the values to be stored
		if(ClusterUtil.IS_CLUSTER) {
			if(workspaceIds != null) {
				for(String workspaceAppId : workspaceIds) {
					try {
						ClusterUtil.pushUserWorkspace(workspaceAppId, false);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			if(assetIds != null) {
				for(String assetAppId : assetIds) {
					try {
						ClusterUtil.pushUserWorkspace(assetAppId, true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

}
