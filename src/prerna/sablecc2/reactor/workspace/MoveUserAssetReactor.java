package prerna.sablecc2.reactor.workspace;

import java.io.File;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class MoveUserAssetReactor extends AbstractReactor{

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public MoveUserAssetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.RELATIVE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String currentFilePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		if(currentFilePath == null || currentFilePath.isEmpty()) {
			throw new IllegalArgumentException("Must input file path for the user file");
		}
		currentFilePath = Utility.normalizePath(currentFilePath);

		String newFilePath = this.keyValue.get(this.keysToGet[1]);
		if(newFilePath == null || newFilePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide new file path or name for file");
		}
		newFilePath = Utility.normalizePath(newFilePath);
		
		File currentFile = new File(currentFilePath);
		if(!currentFile.exists()){
			throw new IllegalArgumentException("File does not exist at this location");
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
			throw new IllegalArgumentException("Unable to find Asset App ID for user");
		}
		
		String userFolderPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, true);
		File userFolder = new File(userFolderPath);
		if(!userFolder.exists()){
			throw new IllegalArgumentException("Unable to find user asset app directory");
		}
		
		String newRelativePath = userFolderPath + newFilePath;
		Boolean moved = currentFile.renameTo(new File(newRelativePath));
		return new NounMetadata(moved, PixelDataType.BOOLEAN, PixelOperationType.USER_DIR);
	}
}
