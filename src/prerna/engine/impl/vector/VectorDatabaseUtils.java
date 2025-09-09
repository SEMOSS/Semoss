package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.reactor.frame.gaas.processors.AbstractFileProcessor;
import prerna.reactor.frame.gaas.processors.IFileProcessor;
import prerna.reactor.frame.gaas.processors.PDFProcessor;
import prerna.util.Constants;

public class VectorDatabaseUtils {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseUtils.class);

	/**
	 * 
	 * @param csvFileName
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int convertFilesToCSV(String csvFileName, File file, Boolean processWithImages, Insight insight) throws Exception {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName);
		try {
			classLogger.info("Processing file : " + file.getName());
			IFileProcessor processor = AbstractFileProcessor.getFileProcessor(file, writer);
			if(processor != null) {
				
				// Need to make process with images optional				
				if (processWithImages && processor instanceof PDFProcessor) {
					PDFProcessor pdfProcessor = (PDFProcessor) processor;
					pdfProcessor.processWithImages(insight);
				} else {
					processor.process();
				}
				classLogger.info("Completed Processing file : " + file.getAbsolutePath());
			} else {
				classLogger.info("No file processor for file : " + file.getAbsolutePath());
			}
		} catch(NullPointerException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			writer.close();
		}

		return writer.getRowsInCsv();
	}
	
	/**
	 * 
	 * @param csvFileName
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int convertFilesToCSV(String csvFileName, File file) throws Exception {
		return convertFilesToCSV(csvFileName, file, false, null);
	}

}