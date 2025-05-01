package prerna.engine.impl.vector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

public class VectorDatabaseCSVWriter {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseCSVWriter.class);
	
	private FileOutputStream fw = null;
	private PrintWriter pw = null;

	// takes an input file
	// starts appending CSV to it
	private String filePath = null;
	// this is number of rows not including headers
	private int rowsCreated = 0;
	
	public VectorDatabaseCSVWriter(String filePath) {
		this.filePath = filePath;
		File file = new File(filePath);
		try {
			if(file.exists()) {
				// no need to write headers
				// open in append mode
				fw = new FileOutputStream(file, true);
				pw = new PrintWriter(new OutputStreamWriter(fw, StandardCharsets.UTF_8));
			}
			else
			{
				fw = new FileOutputStream(file, false);
				pw = new PrintWriter(new OutputStreamWriter(fw, StandardCharsets.UTF_8));
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
		// this should always be the first row
		StringBuffer row = new StringBuffer()
				.append("Source").append(",")
				.append("Modality").append(",")
				.append("Divider").append(",")
				.append("Part").append(",")
				.append("Content")
				.append("\r\n");
		this.pw.print(row + "");
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

	/**
	 * divider is page number or slide number etc. 
	 * @param source
	 * @param divider
	 * @param content
	 */
	public void writeRow(String source, String divider, String content)
	{
		StringBuilder row = new StringBuilder()
				.append("\"").append(cleanString(source)).append("\"").append(",")
				.append("\"").append("text").append("\"").append(",")
				.append("\"").append(cleanString(divider)).append("\"").append(",")
				.append("\"").append(0).append("\"").append(",")
				.append("\"").append(cleanString(content)).append("\"")
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
