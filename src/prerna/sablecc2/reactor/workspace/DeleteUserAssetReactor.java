package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteUserAssetReactor extends AbstractReactor {
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();


	public DeleteUserAssetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(),ReactorKeysEnum.FILE_NAME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		
		String fileName = this.keyValue.get(this.keysToGet[1]);

		
		if(relativePath == null || relativePath.isEmpty() || fileName == null || fileName.isEmpty()  ) {
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

		String userFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db" + DIR_SEPARATOR + WorkspaceAssetUtils.ASSET_APP_NAME + "__" +  assetEngineID ;

		File relativeFolder = new File(userFolder + DIR_SEPARATOR + relativePath);
		
		//File to delete can be a folder so check for that
		File toDelete = new File(relativeFolder + DIR_SEPARATOR + fileName);
		
		if(!toDelete.exists()){
			throw new IllegalArgumentException("File/Folder does not exist that this location");

		}
		
		
		Boolean deleted=false;
		if(toDelete.isDirectory()){
			try {
				FileUtils.deleteDirectory(toDelete);
				deleted = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else{
			deleted = toDelete.delete();
		}
		
		//When i get appId
		//ClusterUtil.reactorPushApp(appId);

		
		return new NounMetadata(deleted, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
	}
}
