package prerna.sablecc2.reactor.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ToCsvReactor extends TaskBuilderReactor {

	private static final String CLASS_NAME = ToCsvReactor.class.getName();
	private String fileLocation = null;
	private Logger logger;
	
	@Override
	public NounMetadata execute() {
		logger = getLogger(CLASS_NAME);
		this.task = getTask();
		this.fileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".csv";
		buildTask();
		return new NounMetadata(this.fileLocation, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
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
