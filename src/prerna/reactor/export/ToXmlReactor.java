package prerna.reactor.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ToXmlReactor extends AbstractExportTxtReactor {

	private static final Logger logger = LogManager.getLogger(ToXmlReactor.class);

	public ToXmlReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), APPEND_TIMESTAMP };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		this.task = getTask();
		this.appendTimestamp = appendTimeStamp();

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);

		// get a random file name
		String prefixName = Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = getExportFileName(user, prefixName, "xml", this.appendTimestamp);

		// grab file path to write the file
		this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (this.fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			File f = new File(insightFolder);
			if (!f.exists()) {
				f.mkdirs();
			}
			this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			this.fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}

		insightFile.setFilePath(this.fileLocation);
		buildTask();

		// store the insight file
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the xml file"));
		return retNoun;
	}

	@Override
	protected void buildTask() {
		// TODO Auto-generated method stub
		// TODO: consolidate with Utility writeResultToFile?
		// TODO: consolidate with Utility writeResultToFile?
		// TODO: consolidate with Utility writeResultToFile?
		// TODO: consolidate with Utility writeResultToFile?
		// TODO: consolidate with Utility writeResultToFile?
		// TODO: consolidate with Utility writeResultToFile?
		// TODO: consolidate with Utility writeResultToFile?

		File f = new File(this.fileLocation);
		try {
			// optimize the query so that it matches the general results on FE
			try {
				this.task.optimizeQuery(-1);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred generating the query to write to the file");
			}

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
				StringBuilder builder = new StringBuilder();
				// create typesArr as an array for faster searching
				String[] headers = null;
				SemossDataType[] typesArr = null;

				int counter = 1;
				// now loop through all the data
				while (this.task.hasNext()) {
					IHeadersDataRow row = this.task.next();
					builder = new StringBuilder();
					List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();
					if (counter == 1) {
						builder.append("<DataTable>").append("\n");

						i = 0;
						headers = row.getHeaders();
						size = headers.length;
						typesArr = new SemossDataType[size];
						for (; i < size; i++) {
							if (headerInfo.get(i).containsKey("dataType")) {
								typesArr[i] = SemossDataType
										.convertStringToDataType(headerInfo.get(i).get("dataType").toString());
							} else if (headerInfo.get(i).containsKey("type")) {
								typesArr[i] = SemossDataType
										.convertStringToDataType(headerInfo.get(i).get("type").toString());
							} else {
								typesArr[i] = SemossDataType.STRING;
							}
						}
					}
					
					// generate the data row
					Object[] dataRow = row.getValues();
					i = 0;
					String currTable = null;
					for (; i < size; i++) {
						String[] rowHeaderInfo = headerInfo.get(i).get("header").toString().split("__");
						String tab = rowHeaderInfo[0];  
						String col = rowHeaderInfo[1]; 
						if (!tab.equals(currTable)) {
							if (currTable != null) {
								builder.append("</").append(currTable).append(">");
							}
							currTable = tab.toString();
							builder.append("<").append(tab).append(">");
						}
						builder.append("<").append(col).append(">");

						if (Utility.isNullValue(dataRow[i])) {
							builder.append("null");
						} else {
							if (typesArr[i] == SemossDataType.STRING) {
								builder.append("\"").append(dataRow[i].toString().replace("\"", "\"\"")).append("\"");
							} else {
								builder.append(dataRow[i]);
							}
						}
						builder.append("</").append(col).append(">");
					}

					if (noTable) {
						builder.append("\n");
					} else {
						builder.append("</").append(currTable).append(">").append("\n");
					}
					// write row to file
					bufferedWriter.write(builder.toString());

					if (counter % 10_000 == 0) {
						logger.info("Finished writing line " + counter);
					}
					counter++;
				}
				builder = new StringBuilder();
				builder.append("</DataTable>");
				bufferedWriter.write(builder.toString());
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
			if (f.exists()) {
				f.delete();
			}
			throw new IllegalArgumentException(e.getMessage(), e);
		} finally {
			if (this.task != null) {
				try {
					this.task.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}
