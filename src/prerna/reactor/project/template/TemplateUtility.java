package prerna.reactor.project.template;

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

import prerna.project.api.IProject;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class TemplateUtility {

	private static final Logger logger = LogManager.getLogger(TemplateUtility.class);
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static final String TEMPLATE_PROPS_FILE = "template.properties";
	public static final String TEMPLATE = "template";

	/**
	 * This method is used to fetch the list of templates from the corresponding App
	 * template location
	 * 
	 * @param projectId
	 * @return
	 */
	public static Map<String, String> getTemplateList(String projectId) {
		Map<String, String> templateDataMap = new HashMap<>();
		IProject project = Utility.getProject(projectId);
		// fetching the project base folder based on the app id
		String projectName = project.getProjectName();
		// fetching the project base folder based on the app id
		String assetFolder = AssetUtility.getProjectAssetFolder(projectName, projectId).replace('\\', '/'); 

		File file = new File(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE); 
		if (!file.exists()) {
			file.getParentFile().mkdirs();
			try {
				file.createNewFile();
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		Properties props = Utility.loadProperties(file.getAbsolutePath());
		props.forEach((k, v) -> templateDataMap.put((String) k, (String) v));

		// templateDataMap will contain all the template information 
		// with template name as key and file name as the value 
		return templateDataMap;
	}

	/**
	 * This method will return the complete template file location based on the
	 * template file
	 * 
	 * @param projectId
	 * @param templateName
	 * @return
	 */
	public static String getTemplateFile(String projectId, String templateName) {
		String assetFolder = AssetUtility.getProjectBaseFolder(projectId).replace('\\', '/');
		String fileName = getTemplateList(projectId).get(templateName);
		// returns the project template folder appended with the template file name 
		if(fileName.startsWith("/") || fileName.startsWith("\\")) {
			return assetFolder + fileName;
		} else {
			return assetFolder + DIR_SEPARATOR + fileName;
		}
	}

	/**
	 * This method will delete the template information from property file and
	 * delete the template file for the corresponding template name
	 * 
	 * @param projectId
	 * @param templateRelativeFilePath
	 * @param templateName
	 * @return
	 */
	public static Map<String, String> deleteTemplate(String projectId, String templateRelativeFilePath, String templateName) {
		Map<String, String> templateDataMap = new HashMap<>();
		
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		// fetching the project asset folder 
		String assetFolder = AssetUtility.getProjectAssetFolder(projectName, projectId).replace('\\', '/');
		templateRelativeFilePath = templateRelativeFilePath.replace('\\', '/');
		// deleting the corresponding template file by appending 
		// the template folder and filename to the project asset folder
		File file = null;
		if(templateRelativeFilePath.startsWith("/") || templateRelativeFilePath.startsWith("\\")) {
			file = new File(assetFolder + templateRelativeFilePath);
		} else {
			file = new File(assetFolder + DIR_SEPARATOR + templateRelativeFilePath);
		}
		file.delete();

		File templatePropsFile = new File(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
		Properties templateProps = Utility.loadProperties(templatePropsFile.getAbsolutePath());
		
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(templatePropsFile.getAbsolutePath());
			
			// removing the template from the properties due to deletion of the template
			templateProps.remove(templateName);
			// rewriting to the template property file the updated properties 
			templateProps.store(out, null);
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			templateProps.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if(out != null) {
					out.close();
				}
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		
		// returning back the updated template information which will contain all the template information
		// with template name as key and file name as the value
		return templateDataMap;
	}

	/**
	 * This method will add a new template file and update the template information
	 * in template property file
	 * 
	 * @param projectId
	 * @param filename
	 * @param templateName
	 * @return
	 */
	public static Map<String, String> addTemplate(String projectId, String filename, String templateName) {
		Map<String, String> templateDataMap = new HashMap<>();
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		// fetching the project asset folder
		String assetFolder = AssetUtility.getProjectAssetFolder(projectName, projectId);
		assetFolder = assetFolder.replace('\\', '/');

		File templatePropsFile = new File(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
		if(!templatePropsFile.exists()) {
			templatePropsFile.getParentFile().mkdirs();
			try {
				templatePropsFile.createNewFile();
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		Properties templateProps = Utility.loadProperties(templatePropsFile.getAbsolutePath());

		FileOutputStream out = null;
		try {
			if (templateProps.containsKey(templateName) && !filename.isEmpty()) {
				File fileToDelete = new File(assetFolder + filename);
				// deleting the corresponding template file by appending the template folder and
				// filename to the app asset folder before throwing exception
				fileToDelete.delete();
				throw new SemossPixelException("Template Name already exists");
			}
			out = new FileOutputStream(templatePropsFile.getAbsolutePath());
			// adding the template from the properties due to addition of the template
			templateProps.put(templateName, filename);
			// rewriting to the template property file the updated properties
			templateProps.store(out, null);
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			templateProps.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
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
	 * @param projectId
	 * @param templateRelativeFilePath
	 * @param templateName
	 * @return
	 */
	public static Map<String, String> editTemplate(String projectId, String templateRelativeFilePath, String templateName) {
		Map<String, String> templateDataMap = new HashMap<>();
		
		IProject project = Utility.getProject(projectId);
		String projectName = project.getProjectName();
		// fetching the project asset folder 
		String assetFolder = AssetUtility.getProjectAssetFolder(projectName, projectId).replace('\\', '/');
		templateRelativeFilePath = templateRelativeFilePath.replace('\\', '/');
		
		// get the properties file 
		File templatePropsFile = new File(assetFolder + DIR_SEPARATOR + TEMPLATE + DIR_SEPARATOR + TEMPLATE_PROPS_FILE);
		Properties templateProps = Utility.loadProperties(templatePropsFile.getAbsolutePath());

		// need to grab the existing file to delete from the prop file
		String removeTemplateRelativePath = (String) templateProps.get(templateName);
		// deleting the corresponding template file by appending 
		// the template folder and filename to the app asset folder
		File file = null;
		if(removeTemplateRelativePath.startsWith("/") || removeTemplateRelativePath.startsWith("\\")) {
			file = new File(Utility.normalizePath(assetFolder + removeTemplateRelativePath));
		} else {
			file = new File(Utility.normalizePath(assetFolder + DIR_SEPARATOR + removeTemplateRelativePath));
		}
		file.delete();
		
		// now we will update the prop file to point to the new file
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(templatePropsFile.getAbsolutePath());
			// deleting the old template information and adding the updated template information 
			// from the properties due to updating of the template
			templateProps.remove(templateName);
			templateProps.put(templateName, templateRelativeFilePath);
			
			// rewriting to the template property file the updated properties
			templateProps.store(out, null);
			// iterate through the properties and update in the templateDataMap with 
			// Key(k) as template name and Value (v) as file name
			templateProps.forEach((k, v) -> templateDataMap.put((String) k, (String) v));
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(out != null) {
				try {
					out.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// returning back the updated template information which will contain all the template information 
		// with template name as key and file name as the value
		return templateDataMap;
	}

	/**
	 * This method will fetch the placeholder information from the placeholder sheet
	 * of the corresponding template
	 * 
	 * @param projectId
	 * @param templateName
	 * @return
	 */
	public static Map<String, List<String>> getPlaceHolderInfo(String projectId, String templateName) {
		Map<String, List<String>> placeHolderData = new HashMap<String, List<String>>();
		FileInputStream fis = null;
		XSSFWorkbook wb = null;
		try {
			String exportTemplateFile = getTemplateFile(projectId, templateName);
			 // fetching the template file 
			File file = new File(exportTemplateFile);
			fis = new FileInputStream(file);
			// creating Workbook instance that refers to template .xlsx file
			wb = new XSSFWorkbook(fis);
			// creating a Sheet object to retrieve place holder sheet
			 // creating a Sheet object to retrieve place holder sheet
			XSSFSheet placeholderSheet = wb.getSheet("placeholders");
			if (placeholderSheet != null) {
				placeHolderData = extractPlaceHolderInfo(placeholderSheet);
			}
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wb != null) {
				try {
					wb.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		// returns the complete place holder data with key as placeholder label name and values 
		// containing place holder default value, cell position
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
		if(rows.hasNext()) {
			rows.next(); // skips the first row as its a header label
		}
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
			// retrieve the label placeholder position from cell index 2 from place holder sheet
			if(row.getCell(3) != null) {
				placeholderPosition = row.getCell(3).getStringCellValue();
				placeholderValueAndPosition.add(placeholderPosition);
			}
			if(!(placeholderName.isEmpty() || placeholderPosition.isEmpty())) {
				placeHolderData.put(placeholderName, placeholderValueAndPosition);
			}
		}
		
		// returns the complete place holder data with key as placeholder label name and values 
		// containing place holder default value, cell position
		return placeHolderData;
	}

}
