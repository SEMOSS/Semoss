package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.frame.gaas.processors.AbstractFileProcessor;
import prerna.reactor.frame.gaas.processors.IFileProcessor;

public class VectorDatabaseUtils {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseUtils.class);

	/**
	 * 
	 * @param csvFileName
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int convertFilesToCSV(String csvFileName, File file) throws Exception {
		try (VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName)) {
			classLogger.info("Processing file : " + file.getName());
			IFileProcessor processor = AbstractFileProcessor.getFileProcessor(file, writer);
			if (processor != null) {
				processor.process();
				classLogger.info("Completed Processing file : " + file.getAbsolutePath());
			} else {
				classLogger.info("No file processor for file : " + file.getAbsolutePath());
			}
			return writer.getRowsInCsv();
		}
	}

}