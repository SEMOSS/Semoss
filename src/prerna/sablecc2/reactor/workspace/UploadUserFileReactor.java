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
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String uploadedFilePath = this.keyValue.get(this.keysToGet[0]);
		if(uploadedFilePath == null || uploadedFilePath.isEmpty()) {
			throw new IllegalArgumentException("Must input file path for the user file");
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

		String userFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db" + DIR_SEPARATOR + WorkspaceAssetUtils.ASSET_APP_NAME + "__" +  assetEngineID ;

		File userFolder = new File(userFolderPath);
		
		if(!userFolder.exists()){
			throw new IllegalArgumentException("Unable to find user asset app directory");
		}
		
		String fileName = uploadedFile.getName().toLowerCase();
		//copy file into the directory from tmp upload space if it is valid. For now its just .R files
		if(!fileName.toLowerCase().endsWith(".r") || fileName.toLowerCase().endsWith(".py")) {
			throw new IllegalArgumentException("File must be of type .r or .py");
		}

		
		try {
			FileUtils.copyFile(uploadedFile, new File(userFolder.getAbsolutePath() + DIR_SEPARATOR + uploadedFile.getName()));
			if(ClusterUtil.IS_CLUSTER) {
				try {
					CloudClient.getClient().pushApp(assetEngineID);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
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
