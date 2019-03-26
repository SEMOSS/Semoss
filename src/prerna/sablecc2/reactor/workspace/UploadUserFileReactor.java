package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

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

public class UploadUserFileReactor extends AbstractReactor {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public UploadUserFileReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.RELATIVE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String uploadedFilePath = this.keyValue.get(this.keysToGet[0]);
		if(uploadedFilePath == null || uploadedFilePath.isEmpty()) {
			throw new IllegalArgumentException("Must input file path for the user file");
		}

		String relativeFilePath = this.keyValue.get(this.keysToGet[1]);
		if(relativeFilePath == null || relativeFilePath.isEmpty()) {
			relativeFilePath = "";
		}

		File uploadedFile = new File(uploadedFilePath);

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
			throw new IllegalArgumentException("Unable to find Asset App ID for user");
		}

		String baseUserFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db" + DIR_SEPARATOR +
				WorkspaceAssetUtils.ASSET_APP_NAME + "__" +  assetEngineID ;

		File baseUserFolder = new File(baseUserFolderPath);

		if(!baseUserFolder.exists()){
			throw new IllegalArgumentException("Unable to find user asset app directory");
		}

		//Where we are storing their information under version. Make the version folder if it doesn't exist.
		String userFolderPath =  baseUserFolderPath + DIR_SEPARATOR + "version";
		File userFolder = new File(userFolderPath);
		Boolean newFolder = userFolder.mkdir();
		if (ClusterUtil.IS_CLUSTER){
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

		String fileName = uploadedFile.getName().toLowerCase();
		//copy file into the directory from tmp upload space if it is valid. For now its just .R files
		if(!fileName.toLowerCase().endsWith(".r") || fileName.toLowerCase().endsWith(".py")) {
			throw new IllegalArgumentException("File must be of type .r or .py");
		}


		try {
			FileUtils.copyFile(uploadedFile, new File(userFolder.getAbsolutePath() + DIR_SEPARATOR + relativeFilePath + DIR_SEPARATOR + uploadedFile.getName()));
			ClusterUtil.reactorPushApp(assetEngineID);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to copy file");
		}


		Map<String, Object> uploadUserData = new HashMap<String, Object>();
		uploadUserData.put("uploadedFile", uploadedFilePath);
		uploadUserData.put("pushedContainer", assetEngineID);
		return new NounMetadata(uploadUserData, PixelDataType.MAP, PixelOperationType.USER_UPLOAD);
	}

}
