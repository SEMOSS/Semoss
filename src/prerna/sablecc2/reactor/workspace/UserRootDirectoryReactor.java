package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
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
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		
		Boolean isRoot = false;
		if(relativePath == null || relativePath.isEmpty()) {
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

		String userFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db" + DIR_SEPARATOR + WorkspaceAssetUtils.ASSET_APP_NAME + "__" +  assetEngineID ;

		
		File userFolder = new File(userFolderPath);
		if(!userFolder.isDirectory()){
			throw new IllegalArgumentException("Folder does not exist");
		}
		File[] userFiles = userFolder.listFiles();
		int numFiles = userFiles.length;
		
		String[] fileNames = new String[numFiles];
		boolean[] isDir = new boolean[numFiles];
		for(int i = 0; i < numFiles; i++) {
			if(userFiles[i].getName().equalsIgnoreCase("hidden.semoss")){
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
