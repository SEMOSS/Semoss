/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.aws.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/** Deprecated on March 21st 2025. Please use StorageEngine Directly */
@Deprecated
public class S3UploaderReactor extends TaskBuilderReactor {

	private static final String CLASS_NAME = S3UploaderReactor.class.getName();
	private static final String FILE_NAME = "fileName";
	private static final String BUCKET = "bucket";

	private String fileLocation = null;
	private Logger logger;

	public S3UploaderReactor() {
		this.keysToGet = S3Utils.addCommonS3Keys(new String[]{FILE_NAME, BUCKET});
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (key.equals(FILE_NAME)) {
			return "Base file name to use for S3 object";
		} else if (key.equals(BUCKET)) {
			return "S3 bucket name";
		} else {
			String commonDescription = S3Utils.getDescriptionForCommonS3Key(key);
			if (commonDescription != null) {
				return commonDescription;
			}
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public String getReactorDescription() {
		return "Upload task data as a CSV to an S3 bucket. Credentials can be set via a profile path/name or with an explicit access key and secret";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String fileName = keyValue.get(keysToGet[0]);
		String bucketName = this.keyValue.get(this.keysToGet[1]);
		if (fileName == null || fileName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file name");
		}
		if (bucketName == null || bucketName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify bucket");
		}

		logger = getLogger(CLASS_NAME);
		this.task = getTask();
		this.fileLocation = Utility.getBaseFolder() + DIR_SEPARATOR + fileName + ".csv";

		// make file
		buildTask();

		AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);
		File fileToPush = new File(Utility.normalizePath(this.fileLocation));
		TransferManager xferMgr = TransferManagerBuilder.standard().withS3Client(s3Client).build();
		boolean transferFailure = false;
		try {
			Upload xfer = xferMgr.upload(bucketName, fileToPush.getName(), fileToPush);
			xfer.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			logger.error("Amazon upload failure: " + e.getMessage());
			transferFailure = true;
		}
		xferMgr.shutdownNow();

		if (transferFailure) {
			return getError("Error occurred during upload");
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.SUCCESS);
	}

	@Override
	protected void buildTask() {
		File f = new File(this.fileLocation);

		try {
			long start = System.currentTimeMillis();

			try {
				f.createNewFile();
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
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
				if (this.task.hasNext()) {
					IHeadersDataRow row = this.task.next();
					List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();

					// generate the header row
					// and define constants used throughout like size, and types
					i = 0;
					headers = row.getHeaders();
					size = headers.length;
					typesArr = new SemossDataType[size];
					builder = new StringBuilder();
					for (; i < size; i++) {
						builder.append("\"").append(headers[i]).append("\"");
						if ((i + 1) != size) {
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
					for (; i < size; i++) {
						if (typesArr[i] == SemossDataType.STRING) {
							builder.append("\"").append(dataRow[i]).append("\"");
						} else {
							builder.append(dataRow[i]);
						}
						if ((i + 1) != size) {
							builder.append(",");
						}
					}
					// write row to file
					bufferedWriter.write(builder.append("\n").toString());
				}

				int counter = 1;
				// now loop through all the data
				while (this.task.hasNext()) {
					IHeadersDataRow row = this.task.next();
					// generate the data row
					Object[] dataRow = row.getValues();
					builder = new StringBuilder();
					i = 0;
					for (; i < size; i++) {
						if (typesArr[i] == SemossDataType.STRING) {
							builder.append("\"").append(dataRow[i]).append("\"");
						} else {
							builder.append(dataRow[i]);
						}
						if ((i + 1) != size) {
							builder.append(",");
						}
					}
					// write row to file
					bufferedWriter.write(builder.append("\n").toString());

					if (counter % 10_000 == 0) {
						logger.info("Finished writing line " + counter);
					}
					counter++;
				}

			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				try {
					if (bufferedWriter != null) {
						bufferedWriter.close();
					}
					if (writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}

			long end = System.currentTimeMillis();
			logger.info("Time to output file = " + (end - start) + " ms");
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			if (f.exists()) {
				f.delete();
			}
			throw new IllegalArgumentException("Encountered error while writing to CSV file");
		}
	}
}
