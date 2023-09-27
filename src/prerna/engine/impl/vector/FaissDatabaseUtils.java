package prerna.engine.impl.vector;

import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.reactor.frame.gaas.processors.CSVWriter;
import prerna.sablecc2.reactor.frame.gaas.processors.DocProcessor;
import prerna.sablecc2.reactor.frame.gaas.processors.PDFProcessor;
import prerna.sablecc2.reactor.frame.gaas.processors.PPTProcessor;

public class FaissDatabaseUtils {
	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseUtils.class);
	
	public static void convertFilesToCSV(String csvFileName, int contentLength, int contentOverlap, List <String> fileNames) throws IOException {
		CSVWriter writer = new CSVWriter(csvFileName);
		writer.setTokenLength(contentLength);
		writer.overlapLength(contentOverlap);

		classLogger.info("Starting file conversions ");
		List <String> processedList = new ArrayList<String>();

		// pick up the files and convert them to CSV
		for(int fileIndex = 0; fileIndex < fileNames.size(); fileIndex++) {
			String thisFile = fileNames.get(fileIndex);
			classLogger.info("Processing file : " + thisFile);

			String fileLocation = thisFile;		
			Path filePath = Paths.get(fileLocation);
			// process this file
			String mimeType = null;
			if (SystemUtils.IS_OS_MAC) {
			     mimeType = URLConnection.guessContentTypeFromName(filePath.toFile().getName());

			} else {
				 mimeType = Files.probeContentType(filePath);
			}
			if(mimeType != null) {
				if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document"))
				{
					// document
					DocProcessor dp = new DocProcessor(fileLocation, writer);
					dp.process();
					processedList.add(thisFile);
				}
				else if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation"))
				{
					// powerpoint
					PPTProcessor pp = new PPTProcessor(fileLocation, writer);
					pp.process();
					processedList.add(thisFile);
				}
				else if(mimeType.equalsIgnoreCase("application/pdf"))
				{
					PDFProcessor pdf = new PDFProcessor(fileLocation, writer);
					pdf.process();
					processedList.add(thisFile);
				}
				else
				{
					classLogger.warn("We Currently do not support mime-type " + thisFile);
				}
				classLogger.info("Completed Processing file : " + thisFile);
			}
		}
	}
	
	
	
}
