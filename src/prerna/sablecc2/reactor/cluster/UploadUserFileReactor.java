package prerna.sablecc2.reactor.cluster;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

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
		String filePath = this.keyValue.get(this.keysToGet[0]);
				
		Map<String, Object> uploadUserData = new HashMap<String, Object>();

		String userID = this.insight.getUserId();

		
		if(!ClusterUtil.IS_CLUSTER){
			throw new IllegalArgumentException("SEMOSS is not in clustered mode");
		}
		
		if(filePath == null || filePath.isEmpty()) {
			throw new IllegalArgumentException("Must input file path");
		}
		if(userID == null || userID.isEmpty()) {
			throw new IllegalArgumentException("Must have a user to push file");
		}
		
		String file_seperator = System.getProperty("file.separator");
		File file = new File(filePath);
		String userSpace = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + file_seperator + "users";

		
		CloudClient.getClient();
		String validID = CloudClient.cleanID(userID);
		
		//make the directory if it doesn't exist
		File userFolder = new File(userSpace + file_seperator + validID);
		if(!userFolder.exists()){
			try {
				CloudClient.getClient().pullUser(userID);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//if user folder still doesn't exist, throw an exception 
		
		if(!userFolder.exists()){
			throw new IllegalArgumentException("Unable to create user director for user: "+ userID);

		}
		
		
		//copy file into the directory from tmp upload space if it is valid. For now its just .R files
		if(file.getAbsolutePath().endsWith(".R") || file.getAbsolutePath().endsWith(".r") || file.getAbsolutePath().endsWith(".py")) {
			try {
				FileUtils.copyFile(file, new File(userFolder+file_seperator+file.getName()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new IllegalArgumentException("Unable to copy file");
			}
		} else{
			throw new IllegalArgumentException("File must be of type .R, .r, or .py");
		}
		
		try {
			CloudClient.getClient().pushUser(userID);
			uploadUserData.put("uploadedFile", filePath);
			uploadUserData.put("pushedContainer", validID);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return new NounMetadata(uploadUserData, PixelDataType.MAP, PixelOperationType.USER_UPLOAD);
	}

}
