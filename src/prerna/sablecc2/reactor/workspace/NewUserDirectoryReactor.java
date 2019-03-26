package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.io.IOException;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NewUserDirectoryReactor extends AbstractReactor {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();


	public NewUserDirectoryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELATIVE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);



		if(relativePath == null || relativePath.isEmpty() ) {
			throw new IllegalArgumentException("Must input file path and file name to delete");
		}


		String assetEngineID = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			if(user != null){
				AuthProvider token = user.getPrimaryLogin();
				if(token != null){
					assetEngineID = user.getAssetEngineId(token);
					Utility.getEngine(assetEngineID);
				}
			}
		}

		if(assetEngineID == null){
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String userFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db" + DIR_SEPARATOR + 
				WorkspaceAssetUtils.ASSET_APP_NAME + "__" +  assetEngineID + DIR_SEPARATOR +  "version";

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
				ClusterUtil.reactorPushApp(assetEngineID);
			}

		}


		return new NounMetadata(created, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
	}

}
