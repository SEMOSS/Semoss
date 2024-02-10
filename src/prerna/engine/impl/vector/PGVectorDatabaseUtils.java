package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;

import prerna.ds.py.TCPPyTranslator;
import prerna.reactor.frame.gaas.processors.CSVWriter;
import prerna.reactor.frame.gaas.processors.DocProcessor;
import prerna.reactor.frame.gaas.processors.PDFProcessor;
import prerna.reactor.frame.gaas.processors.PPTProcessor;
import prerna.reactor.frame.gaas.processors.TextFileProcessor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class PGVectorDatabaseUtils {
	private static final Logger classLogger = LogManager.getLogger(PGVectorDatabaseUtils.class);
	
	public static int convertFilesToCSV(String csvFileName, int contentLength, int contentOverlap, File file, TCPPyTranslator vectorPyt) throws IOException {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName);
		writer.setTokenLength(contentLength);
		writer.overlapLength(contentOverlap);
//		writer.setFaissDbVarName(faissDbVarName);
		writer.setPyTranslator(vectorPyt);

		classLogger.info("Starting file conversions ");
		List <String> processedList = new ArrayList<String>();

		// pick up the files and convert them to CSV
		

		classLogger.info("Processing file : " + file.getName());
		
		Path filePath = Paths.get(file.getAbsolutePath());
		// process this file
		String mimeType = null;
		
		//using tika for mime type check since it is more consistent across env + rhel OS and macOS
		Tika tika = new Tika();

		mimeType = tika.detect(filePath);

		if(mimeType != null) {
			classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
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
			else if(mimeType.equalsIgnoreCase("text/plain"))
			{
				TextFileProcessor text = new TextFileProcessor(file.getAbsolutePath(), writer);
				text.process();
				processedList.add(file.getAbsolutePath());
			}
			else
			{
				classLogger.warn("We Currently do not support mime-type " + mimeType);
			}
			classLogger.info("Completed Processing file : " + file.getAbsolutePath());
		
		}
		writer.close();
		
		return writer.getRowsInCsv();
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
	
	public static Set<String> createKeywordsFromChunks(List<String> filePaths, TCPPyTranslator vectorPyt) {
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

	public static Connection getDatabaseConnection(Properties smssProp) {
		Map<String, Object> connectionDetailsMap = new HashMap<String, Object>();
		connectionDetailsMap.put(AbstractSqlQueryUtil.HOSTNAME, smssProp.getProperty(Constants.PGVECTOR_HOSTNAME));
		connectionDetailsMap.put(AbstractSqlQueryUtil.PORT, smssProp.getProperty(Constants.PGVECTOR_PORT));
		connectionDetailsMap.put(AbstractSqlQueryUtil.DATABASE, smssProp.getProperty(Constants.PGVECTOR_DATABASE_NAME));
		connectionDetailsMap.put(AbstractSqlQueryUtil.SCHEMA, smssProp.getProperty(Constants.PGVECTOR_SCHEMA));
		connectionDetailsMap.put(AbstractSqlQueryUtil.USERNAME, smssProp.getProperty(Constants.PGVECTOR_USERNAME));
		connectionDetailsMap.put(AbstractSqlQueryUtil.PASSWORD, smssProp.getProperty(Constants.PGVECTOR_PASSWORD));
		connectionDetailsMap.put(AbstractSqlQueryUtil.TABLE,smssProp.getProperty(Constants.PGVECTOR_TABLE_NAME));
		

		String driver = "POSTGRES";
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);

		String connectionUrl = null;
		Statement statement = null;
		Connection con = null;
		try {
			connectionUrl = queryUtil.setConnectionDetailsfromMap(connectionDetailsMap);
		} catch (RuntimeException e) {
			throw new SemossPixelException(new NounMetadata("Unable to generation connection url with message " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}

		try {
			con = AbstractSqlQueryUtil.makeConnection(queryUtil, connectionUrl, connectionDetailsMap);	
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			String driverError = e.getMessage();
			String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
			errorMessage += driverError;
			errorMessage += " \"";
			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		} finally {
			//todo
		}
		return con;		
	}
}
