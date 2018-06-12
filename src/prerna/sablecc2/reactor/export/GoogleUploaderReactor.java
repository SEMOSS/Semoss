package prerna.sablecc2.reactor.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.RemoteItem;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GoogleUploaderReactor extends TaskBuilderReactor {

	public GoogleUploaderReactor() {
		this.keysToGet = new String[] { "filename" };
	}

	private static final String CLASS_NAME = GoogleUploaderReactor.class.getName();
	private String fileLocation = null;
	private Logger logger;
	private String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
	private String [] beanProps = {"id", "name", "type"}; // add is done when you have a list
	private String jsonPattern = "[id, name, mimeType]";

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String fileName = this.curRow.get(0).toString();

		//String fileName = this.keyValue.get(this.keysToGet[0]);
		if (fileName == null || fileName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file name");
		}
		String accessToken=null;
		User user = this.insight.getUser();
		try{
		if(user==null){
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}


		logger = getLogger(CLASS_NAME);
		this.task = getTask();

		// get a random file name
		this.fileLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + fileName + ".csv";
		//make file
		buildTask();

		//get access token


		//make post for initial metadata
		String url_str="https://www.googleapis.com/drive/v3/files";
		Hashtable params = new Hashtable();
		params.put("name", fileName);
		params.put("mimeType", "text/csv");
		String output = AbstractHttpHelper.makePostCall(url_str, accessToken,params,true);
		RemoteItem upload = (RemoteItem)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		String uploadId=upload.getId();

		//make an update call to the id to add data through binary post
		String url_str2 = "https://www.googleapis.com/upload/drive/v3/files/"+uploadId+"?uploadType=media";
		String output2 = AbstractHttpHelper.makeBinaryFilePatchCall(url_str2, accessToken, this.fileLocation.toString());


		return new NounMetadata(this.fileLocation.toString(), PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	@Override
	protected void buildTask() {
		File f = new File(this.fileLocation);

		try {
			long start = System.currentTimeMillis();

			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}

			FileWriter writer = null;
			BufferedWriter bufferedWriter = null;

			try {
				writer = new FileWriter(f);
				bufferedWriter = new BufferedWriter(writer);

				// store some variables and just reset
				// should be faster than creating new ones each time
				int i = 0;
				int size = 0;
				StringBuilder builder = null;
				// create typesArr as an array for faster searching
				String[] headers = null;
				SemossDataType[] typesArr = null;

				// we need to iterate and write the headers during the first time
				if(this.task.hasNext()) {
					IHeadersDataRow row = this.task.next();
					List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();

					// generate the header row
					// and define constants used throughout like size, and types
					i = 0;
					headers = row.getHeaders();
					size = headers.length;
					typesArr = new SemossDataType[size];
					builder = new StringBuilder();
					for(; i < size; i++) {
						builder.append("\"").append(headers[i]).append("\"");
						if( (i+1) != size) {
							builder.append(",");
						}
						typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
					}
					// write the header to the file
					bufferedWriter.write(builder.append("\n").toString());

					// generate the data row
					Object[] dataRow = row.getValues();
					builder = new StringBuilder();
					i = 0;
					for(; i < size; i ++) {
						if(typesArr[i] == SemossDataType.STRING) {
							builder.append("\"").append(dataRow[i]).append("\"");
						} else {
							builder.append(dataRow[i]);
						}
						if( (i+1) != size) {
							builder.append(",");
						}
					}
					// write row to file
					bufferedWriter.write(builder.append("\n").toString());
				}

				int counter = 1;
				// now loop through all the data
				while(this.task.hasNext()) {
					IHeadersDataRow row = this.task.next();
					// generate the data row
					Object[] dataRow = row.getValues();
					builder = new StringBuilder();
					i = 0;
					for(; i < size; i ++) {
						if(typesArr[i] == SemossDataType.STRING) {
							builder.append("\"").append(dataRow[i]).append("\"");
						} else {
							builder.append(dataRow[i]);
						}
						if( (i+1) != size) {
							builder.append(",");
						}
					}
					// write row to file
					bufferedWriter.write(builder.append("\n").toString());

					if(counter % 10_000 == 0) {
						logger.info("Finished writing line " + counter);
					}
					counter++;
				}

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if(bufferedWriter != null) {
						bufferedWriter.close();
					}
					if(writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			long end = System.currentTimeMillis();
			logger.info("Time to output file = " + (end-start) + " ms");
		} catch(Exception e) {
			e.printStackTrace();
			if(f.exists()) {
				f.delete();
			}
			throw new IllegalArgumentException("Encountered error while writing to CSV file");
		}
	}

}
