package prerna.reactor.workspace;

import java.io.File;
import java.io.IOException;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class NewDirReactor extends AbstractReactor {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/*
	 * TODO:
	 * DONT BELIEVE THIS WORKS WITH CLOUD ? 
	 * 
	 * 
	 */

	public NewDirReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELATIVE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);

		if(relativePath == null || relativePath.isEmpty() ) {
			throw new IllegalArgumentException("Must input file path and file name to delete");
		} else {
			relativePath = Utility.normalizeParam(relativePath);
		}

		String assetProjectId = null;
		User user = this.insight.getUser();
		if(user != null){
			AuthProvider token = user.getPrimaryLogin();
			if(token != null){
				assetProjectId = user.getAssetProjectId(token);
				Utility.getProject(assetProjectId);
			}
		}

		if(assetProjectId == null){
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String userFolder = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, true);
		File relativeFolder = new File(userFolder + DIR_SEPARATOR + relativePath);

		Boolean created = false;
		if(relativeFolder.exists()){
			throw new IllegalArgumentException("There is already a folder at this location with that name");
		} else{
			created = relativeFolder.mkdirs();
			//made folder but now we need to add a hidden file for the cloud 
			if(ClusterUtil.IS_CLUSTER){
				File hidden = new File(relativeFolder+ DIR_SEPARATOR + WorkspaceAssetUtils.HIDDEN_FILE);
				//override created boolean if its cloud to be at the hidden file level
				try {
					created = hidden.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				ClusterUtil.pushEngine(assetProjectId);
			}
		}

		return new NounMetadata(created, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
	}

	public String getName() {
		return "NewDir";
	}

}
