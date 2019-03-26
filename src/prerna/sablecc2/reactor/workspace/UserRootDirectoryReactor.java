package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UserRootDirectoryReactor extends AbstractReactor {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public UserRootDirectoryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELATIVE_PATH.getKey()};
	}


	@Override
	public NounMetadata execute() {

		organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);

		Boolean isRoot = false;
		if(relativePath == null || relativePath.isEmpty() || relativePath.equals("/")) {
			relativePath = "";
			isRoot=true;
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


		//Base Asset Folder. Checking that is exists, otherwise error
		String baseUserFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db" 
				+ DIR_SEPARATOR + WorkspaceAssetUtils.ASSET_APP_NAME + "__" +  assetEngineID;

		File baseUserFolder = new File(baseUserFolderPath);

		//Where we are storing their information under version. Make the version folder if it doesn't exist.
		String userFolderPath =  baseUserFolderPath + DIR_SEPARATOR + "version";
		File userFolder = new File(userFolderPath);
		Boolean newFolder = userFolder.mkdir();
		if(ClusterUtil.IS_CLUSTER){
			if(newFolder){
				File hidden = new File(userFolderPath + DIR_SEPARATOR + WorkspaceAssetUtils.HIDDEN_FILE);
				try {
					hidden.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		//Navigate to the relative path from the version folder
		userFolder = new File(userFolderPath + DIR_SEPARATOR + relativePath);
		File[] userFiles = userFolder.listFiles();
		int numFiles = userFiles.length;

		String[] fileNames = new String[numFiles];
		boolean[] isDir = new boolean[numFiles];
		for(int i = 0; i < numFiles; i++) {
			if(userFiles[i].getName().equalsIgnoreCase(WorkspaceAssetUtils.HIDDEN_FILE)){
				continue;
			}
			fileNames[i] = userFiles[i].getName();
			isDir[i] = userFiles[i].isDirectory();
		}

		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("files", fileNames);
		retMap.put("isDir", isDir);
		retMap.put("isRoot", isRoot);

		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.USER_DIR);
	}

}
