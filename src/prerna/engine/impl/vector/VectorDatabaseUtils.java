package prerna.engine.impl.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.reactor.frame.gaas.processors.DocProcessor;
import prerna.reactor.frame.gaas.processors.ImageDocProcessor;
import prerna.reactor.frame.gaas.processors.PDFProcessor;
import prerna.reactor.frame.gaas.processors.ImagePDFProcessor;
import prerna.reactor.frame.gaas.processors.PPTProcessor;
import prerna.reactor.frame.gaas.processors.ImagePPTProcessor;
import prerna.reactor.frame.gaas.processors.TextFileProcessor;
import prerna.util.Constants;

public class VectorDatabaseUtils {
    
    private static final Logger classLogger = LogManager.getLogger(VectorDatabaseUtils.class);
    
    private static final String DIR_SEPARATOR = "/";
    private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
    
    
	/**
	 * 
	 * @param csvFileName
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int convertFilesToCSV(String csvFileName, File file) throws IOException {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName);
		try {
			classLogger.info("Starting file conversions ");
			List <String> processedList = new ArrayList<String>();
	
			// pick up the files and convert them to CSV
			classLogger.info("Processing file : " + file.getName());
			
			// process this file
			String filetype = FilenameUtils.getExtension(file.getAbsolutePath());
			String mimeType = null;
			
			//using tika for mime type check since it is more consistent across env + rhel OS and macOS
            TikaConfig config = TikaConfig.getDefaultConfig();
            Detector detector = config.getDetector();
            Metadata metadata = new Metadata();
            metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, file.getName());
            try ( TikaInputStream stream = TikaInputStream.get( new FileInputStream(file))) {
                mimeType = detector.detect(stream, metadata).toString();
			} catch (IOException e) {
				classLogger.error(Constants.ERROR_MESSAGE, e);
	        }
			
			if(mimeType != null) {
				classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
				if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
						|| (
								mimeType.equalsIgnoreCase("application/x-tika-ooxml") 
								&& (filetype.equals("doc") || filetype.equals("docx")) 
								)
						)
				{
					// document
					DocProcessor dp = new DocProcessor(file.getAbsolutePath(), writer);
					dp.process();
					processedList.add(file.getAbsolutePath());
				}
				else if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
						|| (
								mimeType.equalsIgnoreCase("application/x-tika-ooxml") 
								&& (filetype.equals("ppt") || filetype.equals("pptx")) 
								)
						)
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
				else if(mimeType.equalsIgnoreCase("text/plain"))
				{
					TextFileProcessor text = new TextFileProcessor(file.getAbsolutePath(), writer);
					text.process();
					processedList.add(file.getAbsolutePath());
				}
				else
				{
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
				}
				classLogger.info("Completed Processing file : " + file.getAbsolutePath());
			
			}
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
    public static Map<String, Object> convertFilesToCSV(String csvFileName, File file, boolean embedImages) throws IOException {
        VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName);
        Map<String, Object> result = new HashMap<>();
        Map<String, String> imageMap = new HashMap<>();
        
        try {
            classLogger.info("Starting file conversions ");
            List <String> processedList = new ArrayList<String>();
    
            // pick up the files and convert them to CSV
            classLogger.info("Processing file : " + file.getName());
            
            // process this file
            String filetype = FilenameUtils.getExtension(file.getAbsolutePath());
            String mimeType = null;
            
            //using tika for mime type check since it is more consistent across env + rhel OS and macOS
            Tika tika = new Tika();
    
            try (FileInputStream inputstream = new FileInputStream(file)) {
                mimeType = tika.detect(inputstream, new Metadata());
            } catch (IOException e) {
                classLogger.error(Constants.ERROR_MESSAGE, e);
            }
            
            if(mimeType != null) {
                classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
                if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                        || (
                                mimeType.equalsIgnoreCase("application/x-tika-ooxml") 
                                && (filetype.equals("doc") || filetype.equals("docx")) 
                                )
                        )
                {
                    if (embedImages) {
                    	ImageDocProcessor idp = new ImageDocProcessor(file.getAbsolutePath(), writer, true);
                    	idp.process();
                    	imageMap = idp.getImageMap();
                    } else {                	
	                    DocProcessor dp = new DocProcessor(file.getAbsolutePath(), writer);
	                    dp.process();
	                    
                    }
                    processedList.add(file.getAbsolutePath());
                    
                }
                else if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
                        || (
                                mimeType.equalsIgnoreCase("application/x-tika-ooxml") 
                                && (filetype.equals("ppt") || filetype.equals("pptx")) 
                                )
                        )
                {
                    // powerpoint
                    if (embedImages) {
                    	ImagePPTProcessor ipp = new ImagePPTProcessor(file.getAbsolutePath(), writer, true);
                    	ipp.process();
                    	imageMap = ipp.getImageMap();
                    } else {                	
	                    PPTProcessor pp = new PPTProcessor(file.getAbsolutePath(), writer);
	                    pp.process();
	                    
                    }
                    processedList.add(file.getAbsolutePath());
                }
                else if(mimeType.equalsIgnoreCase("application/pdf"))
                {
                    
                    // add an if statement whether want to do images or not
                	if (embedImages) {
                        ImagePDFProcessor pdf = new ImagePDFProcessor(file.getAbsolutePath(), writer);
                        pdf.process();
                        imageMap = pdf.getImageMap();
                        processedList.add(file.getAbsolutePath());
                	} else {
                        PDFProcessor pdf = new PDFProcessor(file.getAbsolutePath(), writer);
                        pdf.process();
                        processedList.add(file.getAbsolutePath());
                	}

                }
                else if(mimeType.equalsIgnoreCase("text/plain"))
                {
                    TextFileProcessor text = new TextFileProcessor(file.getAbsolutePath(), writer);
                    text.process();
                    processedList.add(file.getAbsolutePath());
                }
                else
                {
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                    classLogger.warn("No support exists for parsing mime-type = " + mimeType);
                }
                classLogger.info("Completed Processing file : " + file.getAbsolutePath());
            
            }
        } finally {
            writer.close();
        }
        result.put("rowsInCSV", writer.getRowsInCsv());
        result.put("imageMap", imageMap);
        return result;

    }
    
    public static boolean verifyFileTypes(List<String> newFilesPaths, List<String> filesInDocumentsFolder) {
        
        /*
         * First Check
         * Make sure the csv files aren't sent with non csv files
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
    
    /**
     * @param pyTranslator
     * @param document
     * @param extractioFilesPath
     * @param outputFileName
     * @param extractionMethod
     * @return
     */
    public static int extractTextUsingPython(TCPPyTranslator pyTranslator, File document, String extractioFilesPath, String outputFileName) {
        boolean imported = Boolean.parseBoolean(pyTranslator.runScript("'vector_database' in globals().keys()")+"");
        if (!imported) {
            throw new IllegalArgumentException("This vector database does not the vector_database python package.");
        }
        
        StringBuilder extractTextFromDocScript = new StringBuilder();
        extractTextFromDocScript.append("vector_database.extract_text(source_file_name = '")
                             .append(document.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR))
                             .append("', target_folder = '")
                             .append(extractioFilesPath)
                             .append("', output_file_name = '")
                             .append(outputFileName)
                             .append("')");
    
        Number rows = (Number) pyTranslator.runScript(extractTextFromDocScript.toString());
        
        return rows.intValue();
    }
    
    /**
     * @param pyTranslator
     * @param csvFileName
     * @param chunkUnitOfMeasurement
     * @param chunkMaxLength
     * @param chunkOverlap
     * @param chunkingStrategy
     */
    public static void createChunksFromTextInPages(TCPPyTranslator pyTranslator, String csvFileName, String chunkUnitOfMeasurement, int chunkMaxLength, int chunkOverlap, String chunkingStrategy) {
        
        StringBuilder splitTextCommand = new StringBuilder();
        splitTextCommand.append("vector_database.split_text(csv_file_location = '")
                        .append(csvFileName)
                        .append("', chunk_unit = '")
                        .append(chunkUnitOfMeasurement)
                        .append("', chunk_size = ")
                        .append(chunkMaxLength)
                        .append(", chunk_overlap = ")
                        .append(chunkOverlap)
                        .append(", chunking_strategy = ")
                        .append(chunkingStrategy)
                        .append(", cfg_tokenizer = cfg_tokenizer)");
        
        pyTranslator.runScript(splitTextCommand.toString());
    }
    
    @SuppressWarnings("unchecked")
    public static List<String> generateKeywordsFromChunks(IModelEngine modelEngine, Insight insight, List<String> chunks, Integer maxKeywords, Integer percentile) {
        
        Map<String, Object> keywordArgs = new HashMap<>();
        
        if (maxKeywords != null) {
            keywordArgs.put("max_keywords", maxKeywords);
        }
        
        if (percentile != null) {
            keywordArgs.put("percentile", maxKeywords);
        }
        
        Object generatedKeywordsObject = modelEngine.model(chunks, insight, keywordArgs);
        
        return (List<String>) generatedKeywordsObject;
    }
}