package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.reactor.frame.gaas.processors.CSVWriter;
import prerna.sablecc2.reactor.frame.gaas.processors.DocProcessor;
import prerna.sablecc2.reactor.frame.gaas.processors.PDFProcessor;
import prerna.sablecc2.reactor.frame.gaas.processors.PPTProcessor;

public class FaissDatabaseUtils {
	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseUtils.class);
	
	public static void convertFilesToCSV(String csvFileName, int contentLength, int contentOverlap, File file) throws IOException {
		CSVWriter writer = new CSVWriter(csvFileName);
		writer.setTokenLength(contentLength);
		writer.overlapLength(contentOverlap);

		classLogger.info("Starting file conversions ");
		List <String> processedList = new ArrayList<String>();

		// pick up the files and convert them to CSV
		

		classLogger.info("Processing file : " + file.getName());
		
		Path filePath = Paths.get(file.getAbsolutePath());
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
				DocProcessor dp = new DocProcessor(file.getAbsolutePath(), writer);
				dp.process();
				processedList.add(file.getAbsolutePath());
			}
			else if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation"))
			{
				// powerpoint
				PPTProcessor pp = new PPTProcessor(file.getAbsolutePath(), writer);
				pp.process();
				processedList.add(file.getAbsolutePath());
			}
			else if(mimeType.equalsIgnoreCase("application/pdf"))
			{
				PDFProcessor pdf = new PDFProcessor(file.getAbsolutePath(), writer);
				pdf.process();
				processedList.add(file.getAbsolutePath());
			}
			else
			{
				classLogger.warn("We Currently do not support mime-type " + file.getAbsolutePath());
			}
			classLogger.info("Completed Processing file : " + file.getAbsolutePath());
		
		}
		writer.close();
	}
	
	public static boolean verifyFileTypes(List<String> newFilesPaths, List<String> filesInDocumentsFolder) {
		
		/*
		 * First Check
		 * Make sure the csv files aren't send with non csv files
		 * TODO refine checks here
		*/
		Set<String> newFileTypes = extractFileTypesFromPaths(newFilesPaths);
        boolean newFilesAreCsv;
        if (newFileTypes.contains("csv") && newFileTypes.size() == 1) {
        	newFilesAreCsv = true;
        } else {
        	newFilesAreCsv = false;
        }
		
        // TODO update this to do a headers check. Maybe the can do the pre-processing beforehand
        // cant process csvs unless the have the same headers
        if (newFileTypes.contains("csv") && !newFilesAreCsv) {
        	return false;
        }
        
		/*
		 * Second Check
		 * Make sure we arent trying to add csv to a mixed file type class 
		 * TODO refine checks here once they above is addressed
		*/
        Set<String> currentFileTypes = extractFileTypesFromPaths(filesInDocumentsFolder);
        
        if (currentFileTypes.size() == 0) {
        	return true;
        }
        
        
        if (newFilesAreCsv && !currentFileTypes.contains("csv")) {
        	return false;
        }
        
		/*
		 * Third Check
		 * Make sure we are not adding mixed files to just csv class
		 * TODO refine checks here once they above is addressed
		*/
        if (!newFilesAreCsv && currentFileTypes.contains("csv") && currentFileTypes.size() == 1) {
        	return false;
        }
		
		return true;
	}
	
	public static Set<String> extractFileTypesFromPaths(List<String> filePaths) {
        Set<String> fileTypes = new HashSet<>();
        for (String filePath : filePaths) {
            // Find the last dot (.) in the file path
            int lastDotIndex = filePath.lastIndexOf(".");
            
            if (lastDotIndex >= 0) {
                // Extract the file extension
                String fileType = filePath.substring(lastDotIndex + 1).toLowerCase();
                fileTypes.add(fileType);
            }
        }
        return fileTypes;
    }
}
