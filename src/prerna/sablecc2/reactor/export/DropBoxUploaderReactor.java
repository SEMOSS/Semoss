package prerna.sablecc2.reactor.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DropBoxUploaderReactor extends TaskBuilderReactor {

	public DropBoxUploaderReactor() {
		this.keysToGet = new String[] { "filename" };
	}

	private static final String CLASS_NAME = DropBoxUploaderReactor.class.getName();
	private String fileLocation = null;
	private Logger logger;

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String fileName = this.curRow.get(0).toString();
		if (fileName == null || fileName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file name");
		}
		
		//get access token
				String accessToken = null;
				User2 user = this.insight.getUser2();
				try{
				if(user==null){
					SemossPixelException exception = new SemossPixelException();
					exception.setContinueThreadOfExecution(false);
					Map<String, Object> retMap = new HashMap<String, Object>();
					retMap.put("type", "dropbox");
					retMap.put("message", "Please login to your DropBox account");
					exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
					throw exception;
				}
				else if (user != null) {
						AccessToken msToken = user.getAccessToken(AuthProvider.DROPBOX.name());
						accessToken=msToken.getAccess_token();
					}
				}
				catch (Exception e) {
						SemossPixelException exception = new SemossPixelException();
						exception.setContinueThreadOfExecution(false);
						Map<String, Object> retMap = new HashMap<String, Object>();
						retMap.put("type", "dropbox");
						retMap.put("message", "Please login to your DropBox account");
						exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
						throw exception;
				}

		logger = getLogger(CLASS_NAME);
		this.task = getTask();

		// get a random file name
		String randomKey = UUID.randomUUID().toString();
		this.fileLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + randomKey + ".csv";
		//make file
		buildTask();

		//make call to dropbox to upload
		String url_str = "https://content.dropboxapi.com/2/files/upload";
		String output = AbstractHttpHelper.makeBinaryFilePostCall(url_str, accessToken, fileName, this.fileLocation.toString());

		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
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
