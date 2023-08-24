package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class DeleteUserAssetReactor extends AbstractReactor {
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();


	public DeleteUserAssetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELATIVE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String relativeFilePath = this.keyValue.get(this.keysToGet[0]);
		if(relativeFilePath == null || relativeFilePath.isEmpty()  ) {
			throw new IllegalArgumentException("Must input file path and file name to delete");
		}
		relativeFilePath = Utility.normalizePath(relativeFilePath);
		
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
		File relativeFile = new File(userFolder + DIR_SEPARATOR + relativeFilePath);
		if(!relativeFile.exists()){
			throw new IllegalArgumentException("File/Folder does not exist that this location");
		}
		
		//File can be a folder so need to take that into account
		Boolean deleted=false;
		if(relativeFile.isDirectory()){
			try {
				FileUtils.deleteDirectory(relativeFile);
				deleted = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
			catch (IllegalArgumentException e) {
				e.printStackTrace();
			}
		} else{
			deleted = relativeFile.delete();
		}
		
		//When i get appId
		ClusterUtil.pushProject(assetProjectId);

		
		return new NounMetadata(deleted, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
	}
}
