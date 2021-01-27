package prerna.sablecc2.reactor.app.template;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IEngine;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/** This class will contain all the utility methods for the template mananagement
 * @author kprasannakumar
 *
 */
public class TemplateUtility {

	public static final String TEMPLATE_PROPS_FILE = "template.properties";
	public static final String TEMPLATE = "template";
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final Logger logger = LogManager.getLogger(TemplateUtility.class.getName());

	/**
	 * This method is used to fetch the list of templates from the corresponding App
	 * template location
	 * 
	 * @param appId
	 * @return
	 */
	public static Map<String, String> getTemplateList(String appId) {
		Map<String, String> templateDataMap;

		templateDataMap = new HashMap<>();
		IEngine engine = Utility.getEngine(appId);
		// fetching the app base folder based on the app id
		String appName = engine.getEngineName();
		String assetFolder = AssetUtility.getAppAssetFolder(appName, appId); // fetching the app base folder based on the app id
		assetFolder = assetFolder.replace('\\', '/');

		FileInputStream fis = null;
		try {
			// read the template.properties file of the corresponding app to fetch the templates list
			File file = new File(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);  
			// creating the file and template root directory if the file does not exists
			if (!file.exists()) {
				file.getParentFile().mkdirs();
				file.createNewFile();
				FileOutputStream fos = new FileOutputStream(file);
				fos.close();
			}
			fis = new FileInputStream(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);

			Properties props = new Properties();
            // loading the template data as props and saving it in the map
			props.load(fis);
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			props.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
			fis.close();
		} catch (IOException e) {
			logger.error("Error in getTemplateList() :" + e.getMessage());
		} finally {

		}
		// templateDataMap will contain all the template information with template name as key and file name as the value 
		return templateDataMap;
	}

	/**
	 * This method returns the template folder location of the corresponding App
	 * 
	 * @param appId
	 * @return
	 */
	public static String getFilePath(String appId) {

		IEngine engine = Utility.getEngine(appId);
		String appName = engine.getEngineName();
		String assetFolder = AssetUtility.getAppAssetFolder(appName, appId);
		assetFolder = assetFolder.replace('\\', '/');
		
		// returning the asset folder appended with the template folder location which contains all the template files
		return assetFolder; // + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR;

	}

	/**
	 * This method will return the complete template file location based on the
	 * template file
	 * 
	 * @param appId
	 * @param templateName
	 * @return
	 */
	public static String getTemplateFile(String appId, String templateName) {

		String assetFolder = getFilePath(appId);
		String fileName = getTemplateList(appId).get(templateName);
		// replace the insight folder
		fileName = fileName.replace("INSIGHT_FOLDER", assetFolder);
		// returns the app template folder appended with the template file name 
		return fileName;

	}

	/**
	 * This method will delete the template information from property file and
	 * delete the template file for the corresponding template name
	 * 
	 * @param appId
	 * @param filename
	 * @param templateName
	 * @return
	 */
	public static Map<String, String> deleteTemplate(String appId, String filename, String templateName) {
		Map<String, String> templateDataMap;

		templateDataMap = new HashMap<>();
		IEngine engine = Utility.getEngine(appId);
		String appName = engine.getEngineName();
		// fetching the app asset folder 
		String assetFolder = AssetUtility.getAppAssetFolder(appName, appId);
		assetFolder = assetFolder.replace('\\', '/');

		File file = null;
		try {
			// deleting the corresponding template file by appending the template folder and filename to the app asset folder
			file = new File(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + filename);
			file.delete();

		} finally {

		}

		try {
			
			// reading the template information from the template properties file
			FileInputStream in = new FileInputStream(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
			Properties props = new Properties();
			props.load(in);
			in.close();

			FileOutputStream out = new FileOutputStream(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
			
			// removing the template from the properties due to deletion of the template
			props.remove(templateName);
			// rewriting to the template property file the updated properties 
			props.store(out, null);
			out.close();
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			props.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
		} catch (FileNotFoundException e) {
			logger.error("Error in deleteTemplate() :" + e.getMessage());
		} catch (IOException e) {
			logger.error("Error in deleteTemplate() :" + e.getMessage());
		} finally {

		}
		
		// returning back the updated template information which will contain all the template information with template name as key and file name as the value
		return templateDataMap;
	}

	/**
	 * This method will add a new template file and update the template information
	 * in template property file
	 * 
	 * @param appId
	 * @param filename
	 * @param templateName
	 * @return
	 */
	public static Map<String, String> addTemplate(String appId, String filename, String templateName) {
		Map<String, String> templateDataMap = new HashMap<>();
		IEngine engine = Utility.getEngine(appId);
		String appName = engine.getEngineName();
		// fetching the app asset folder
		String assetFolder = AssetUtility.getAppAssetFolder(appName, appId);
		assetFolder = assetFolder.replace('\\', '/');

		FileInputStream in = null;
		FileOutputStream out = null;
		try {
			// reading the template information from the template properties file
			in = new FileInputStream(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
			Properties props = new Properties();
			props.load(in);

			out = new FileOutputStream(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
			// adding the template from the properties due to addition of the template
			props.put(templateName, filename);
			// rewriting to the template property file the updated properties
			props.store(out, null);
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			props.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// returning back the updated template information which will contain all the template information with template name as key and file name as the value
		return templateDataMap;
	}

	/**
	 * This method will update an existing template file and update the template
	 * information in template property file
	 * 
	 * @param appId
	 * @param filename
	 * @param templateName
	 * @return
	 */
	public static Map<String, String> editTemplate(String appId, String filename, String templateName) {
		Map<String, String> templateDataMap;

		templateDataMap = new HashMap<>();
		IEngine engine = Utility.getEngine(appId);
		String appName = engine.getEngineName();
		// fetching the app asset folder
		String assetFolder = AssetUtility.getAppAssetFolder(appName, appId);
		assetFolder = assetFolder.replace('\\', '/');

		try {
			
			// reading the template information from the template properties file
			FileInputStream in = new FileInputStream(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
			Properties props = new Properties();
			props.load(in);
			in.close();
			File file = new File(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + props.getProperty(templateName));
			file.delete();
			FileOutputStream out = new FileOutputStream(
					assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
			// deleting the old template information and adding the updated template information from the properties due to updation of the template
			props.remove(templateName);
			props.put(templateName, filename);
			
			// rewriting to the template property file the updated properties
			props.store(out, null);
			out.close();
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			props.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
		} catch (FileNotFoundException e) {
			logger.error("Error in editTemplate() :" + e.getMessage());
		} catch (IOException e) {
			logger.error("Error in editTemplate() :" + e.getMessage());
		} finally {

		}
		
		// returning back the updated template information which will contain all the template information with template name as key and file name as the value
		return templateDataMap;
	}

	/**
	 * This method will fetch the placeholder information from the placeholder sheet
	 * of the corresponding template
	 * 
	 * @param appId
	 * @param templateName
	 * @return
	 */
	public static Map<String, List<String>> getPlaceHolderInfo(String appId, String templateName) {
		// 
		Map<String, List<String>> placeHolderData = new HashMap<String, List<String>>();
		try {
			String exportTemplateFile = getTemplateFile(appId, templateName);
			File file = new File(exportTemplateFile); // fetching the template file 
			FileInputStream fis;

			fis = new FileInputStream(file);
			// creating Workbook instance that refers to template .xlsx file
			XSSFWorkbook wb = new XSSFWorkbook(fis);
			// creating a Sheet object to retrieve place holder sheet
			XSSFSheet placeholderSheet = wb.getSheet("placeholders"); // creating a Sheet object to retrieve place holder sheet
			if (placeholderSheet != null) {
				placeHolderData = extractPlaceHolderInfo(placeholderSheet);
			}
			wb.close();
		} catch (FileNotFoundException e) {
			logger.error("Error in getPlaceHolderInfo() :" + e.getMessage());
		} catch (IOException e) {
			logger.error("Error in getPlaceHolderInfo() :" + e.getMessage());
		} finally {

		}
		// returns the complete place holder data with key as placeholder label name and values containing place holder default value, cell position
		return placeHolderData;
	}

	/**
     * This method will extract all place holder info from the placeholder Sheet.
     * 
      * @param sheet
     * @return
     */
	public static Map<String, List<String>> extractPlaceHolderInfo(XSSFSheet sheet) {
		Map<String, List<String>> placeHolderData = new HashMap<String, List<String>>();
		Iterator<Row> rows = sheet.iterator(); // iterating over place holder sheet
		while (rows.hasNext()) {
			Row row = rows.next();
			List<String> placeholderValueAndPosition = new ArrayList<>();
			String placeholderName = "", placeholderPosition = "";
			// retrieve the label placeholder name from cell index 0 from place holder sheet
			if(row.getCell(0) != null) {
				placeholderName = row.getCell(0).getStringCellValue(); 
			}
			// retrieve the label placeholder default value from cell index 1 from place holder sheet
			if(row.getCell(1) != null) {
				placeholderValueAndPosition.add(row.getCell(1).getStringCellValue());
			} else {
				placeholderValueAndPosition.add("");
			}
			// retrieve the label placeholder position from cell index 2 from place holder sheet
			if(row.getCell(2) != null) {
				placeholderPosition = row.getCell(2).getStringCellValue();
				placeholderValueAndPosition.add(placeholderPosition);
			}
			if(!(placeholderName.isEmpty() || placeholderPosition.isEmpty())) {
				placeHolderData.put(placeholderName, placeholderValueAndPosition);
			}
		}
		
		// returns the complete place holder data with key as placeholder label name and values containing place holder default value, cell position
		return placeHolderData;
	}


}
