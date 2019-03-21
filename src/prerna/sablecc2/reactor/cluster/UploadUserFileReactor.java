package prerna.sablecc2.reactor.cluster;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class UploadUserFileReactor extends AbstractReactor {

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
		
		String file_seperator = System.getProperty("file.separator");
		File uploadedFile = new File(uploadedFilePath);
		String userSpace = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + file_seperator + "users";
		// in case it doesn't exist for some reason
		File userSpaceF = new File(userSpace);
		if(!userSpaceF.exists()) {
			userSpaceF.mkdir();
		}
		
		String fileName = uploadedFile.getName().toLowerCase();
		//copy file into the directory from tmp upload space if it is valid. For now its just .R files
		if(!fileName.toLowerCase().endsWith(".r") || fileName.toLowerCase().endsWith(".py")) {
			throw new IllegalArgumentException("File must be of type .r or .py");
		}
		
		String userSpaceId = null;
		
		if(!ClusterUtil.IS_CLUSTER) {
			// need to set this locally
			if(AbstractSecurityUtils.securityEnabled()) {
				User user = this.insight.getUser();
				// TODO: figure out which user space to put you into
				AuthProvider token = user.getLogins().get(0);
				userSpaceId = token.toString() + "_" + user.getAccessToken(token).getId();
			} else {
				userSpaceId = "anonymous";
			}
			File userFolder = new File(userSpace + file_seperator + userSpaceId);
			
			//copy file into the directory from tmp upload space if it is valid. For now its just .R files
			try {
				FileUtils.copyFile(uploadedFile, new File(userFolder.getAbsolutePath() + file_seperator + uploadedFile.getName()));
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Unable to copy file");
			}
			
		} else {
			User user = this.insight.getUser();
			AuthProvider token = user.getLogins().get(0);
			userSpaceId = token.toString() + "_" + user.getAccessToken(token).getId();
			
			CloudClient.getClient();
			userSpaceId = CloudClient.cleanID(userSpaceId);
			//make the directory if it doesn't exist
			File userFolder = new File(userSpace + file_seperator + userSpaceId);
			if(!userFolder.exists()){
				try {
					CloudClient.getClient().pullUser(userSpaceId);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
	
			// if user folder still doesn't exist, throw an exception 
			if(!userFolder.exists()){
				throw new IllegalArgumentException("Unable to create user director for user: "+ user.getAccessToken(token).getId());
			}
	
			//copy file into the directory from tmp upload space if it is valid. For now its just .R files
			try {
				FileUtils.copyFile(uploadedFile, new File(userFolder.getAbsolutePath() + file_seperator + uploadedFile.getName()));
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Unable to copy file");
			}
	
			try {
				CloudClient.getClient().pushUser(userSpaceId);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		Map<String, Object> uploadUserData = new HashMap<String, Object>();
		uploadUserData.put("uploadedFile", uploadedFilePath);
		uploadUserData.put("pushedContainer", userSpaceId);
		return new NounMetadata(uploadUserData, PixelDataType.MAP, PixelOperationType.USER_UPLOAD);
	}

}
