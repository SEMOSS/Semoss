package prerna.engine.impl.vector.metadata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.date.SemossDate;
import prerna.util.Constants;

public class VectorDatabaseMetadataCSVWriter {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseMetadataCSVWriter.class);
	
	private FileWriter fw = null;
	private PrintWriter pw = null;

	// takes an input file
	// starts appending CSV to it
	private String filePath = null;
	private int rowsCreated;
	
	public VectorDatabaseMetadataCSVWriter(String filePath) {
		this.filePath = filePath;
		File file = new File(filePath);
		try {
			if(file.exists()) {
				// no need to write headers
				// open in append mode
				fw = new FileWriter(file, true);
				pw = new PrintWriter(fw);
			}
			else
			{
				fw = new FileWriter(file, false);
				pw = new PrintWriter(fw);
				writeHeader();
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	
	public int getRowsInCsv() {
		return this.rowsCreated;
	}

	protected void writeHeader() {
		StringBuffer row = new StringBuffer()
				.append(VectorDatabaseMetadataCSVTable.SOURCE).append(",")
				.append(VectorDatabaseMetadataCSVTable.ATTRIBUTE).append(",")
				.append(VectorDatabaseMetadataCSVTable.STR_VALUE).append(",")
				.append(VectorDatabaseMetadataCSVTable.INT_VALUE).append(",")
				.append(VectorDatabaseMetadataCSVTable.NUM_VALUE).append(",")
				.append(VectorDatabaseMetadataCSVTable.BOOL_VALUE).append(",")
				.append(VectorDatabaseMetadataCSVTable.DATE_VAL).append(",")
				.append(VectorDatabaseMetadataCSVTable.TIMESTAMP_VAL)
				.append("\r\n");
		this.pw.print(row + "");
		
		// this should always be the first row
		this.rowsCreated = 1;
	}
	
	/**
	 * 
	 * @param inputString
	 * @return
	 */
	protected String cleanString(String inputString) {
		if(inputString == null) {
			return null;
		}
		inputString = inputString.replace("\n", " ");
		inputString = inputString.replace("\r", " ");
		inputString = inputString.replace("\\", "\\\\");
		inputString = inputString.replace("\"", "'");

		return inputString;
	}
	
	protected String cleanStringRetEmpty(String inputString) {
		if(inputString == null) {
			return "";
		}
		inputString = inputString.replace("\n", " ");
		inputString = inputString.replace("\r", " ");
		inputString = inputString.replace("\\", "\\\\");
		inputString = inputString.replace("\"", "'");

		return inputString;
	}
	
	/**
	 * 
	 * @param metaValues
	 */
	public void bulkWriteRow(Map<String, Map<String, Object>> metaValues) {
		for(String source : metaValues.keySet()) {
			writeSourceRow(source, metaValues.get(source));
		}
	}
	
	/**
	 * 
	 * @param source
	 * @param metadata
	 */
	public void writeSourceRow(String source, Map<String, Object> metadata) {
		for(String attributeName : metadata.keySet()) {
			Object metaValue = metadata.get(attributeName);
			if(metaValue instanceof Integer) {
				writeRow(source, attributeName, null, (Integer) metaValue, null, null, null, null);
			} else if(metaValue instanceof Number) {
				writeRow(source, attributeName, null, null, (Number) metaValue, null, null, null);
			} else if(metaValue instanceof Boolean) {
				writeRow(source, attributeName, null, null, null, (Boolean) metaValue, null, null);
			} else if(metaValue instanceof SemossDate) {
				if(((SemossDate) metaValue).dateHasTimeNotZero()) {
					// assume timestamp
					writeRow(source, attributeName, null, null, null, null, (SemossDate) metaValue, null);
				} else {
					// assume timestamp
					writeRow(source, attributeName, null, null, null, null, null, (SemossDate) metaValue);
				}
			} else {
				// assume string
				writeRow(source, attributeName, metaValue+"", null, null, null, null, null);
			}
		}
	}
	
	public void writeRow(String source, String attributeName, String strValue, Integer intValue, Number numValue,
			Boolean boolValue, SemossDate dateValue, SemossDate timestampValue) {
		StringBuilder row = new StringBuilder()
				.append("\"").append(cleanString(source)).append("\"").append(",")
				.append("\"").append(cleanString(attributeName)).append("\"").append(",")
				.append("\"").append(cleanStringRetEmpty(strValue)).append("\"").append(",")
				.append(intValue).append(",")
				.append(numValue).append(",")
				.append(boolValue).append(",")
				.append("\"").append(dateValue).append("\"").append(",")
				.append("\"").append(timestampValue).append("\"")
				.append("\r\n");
		//System.out.println(row);
		this.pw.print(row.toString());
		//pw.print(separator);
		this.pw.flush();
		this.rowsCreated += 1;
	}
	
	/**
	 * 
	 */
	public void close() {
		if(this.pw != null) {
			this.pw.close();
		}
		if(this.fw != null) {
			try {
				this.fw.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}
}
