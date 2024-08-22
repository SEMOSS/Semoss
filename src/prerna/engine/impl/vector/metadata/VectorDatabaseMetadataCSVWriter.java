package prerna.engine.impl.vector.metadata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

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
		inputString = inputString.replace("\n", " ");
		inputString = inputString.replace("\r", " ");
		inputString = inputString.replace("\\", "\\\\");
		inputString = inputString.replace("\"", "'");

		return inputString;
	}

	
	public void writeRow(String source, String attribute, String strValue, Integer intValue, Number numValue,
			Boolean boolValue, SemossDate dateValue, SemossDate timestampValue) {
		StringBuilder row = new StringBuilder()
				.append("\"").append(cleanString(source)).append("\"").append(",")
				.append("\"").append(cleanString(attribute)).append("\"").append(",")
				.append("\"").append(cleanString(strValue)).append("\"").append(",")
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
