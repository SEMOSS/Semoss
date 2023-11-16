
package prerna.reactor.database.metaeditor.relationships;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.impl.owl.ReadOnlyOWLEngine;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * 
 * @author chpiper
 * DownloadOwlRelationships ( database = "f9b656cc-06e7-4cce-bae8-b5f92075b6da" ) ;
 * This reactor exports the relationships between tables the user has added to the owl file to an excel.
 * getPhysicalRelationships returns these relationships.
 */
public class DownloadOwlRelationshipsReactor extends AbstractReactor {

	static final String START_TABLE = "START_TABLE";
	static final String START_COLUMN = "START_COLUMN";
	static final String TARGET_TABLE = "TARGET_TABLE";
	static final String TARGET_COLUMN = "TARGET_COLUMN";
		
	public DownloadOwlRelationshipsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		
		// Check if database ID is valid
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		databaseId = testDatabaseId(databaseId, true);

		ReadOnlyOWLEngine owlEngine = Utility.getDatabase(databaseId).getOWLEngineFactory().getReadOWL();
		List<String[]> output = owlEngine.getPhysicalRelationships();
		
		String fileLocation = null;
		try {
			String insightFolder = this.insight.getInsightFolder(); 		
			{
				File f = new File(insightFolder);
				if(!f.exists()) {
					f.mkdirs();
				}
			}
			String exportName = getExportFileName("OWL_RELATIONSHIPS", "xlsx");
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			
			createExcelFile(output, userId, fileLocation);

		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error occurred creating the export!";
			if (e.getMessage() != null && !e.getMessage().isEmpty()) {
				error += e.getMessage();
			}
			throw new IllegalArgumentException(error);
		}	
		
		// store it in the insight so the FE can download it
		// only from the given insight
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(fileLocation);
		this.insight.addExportFile(downloadKey, insightFile);
		
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD); 
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the relationship excel file"));
		return retNoun;
	}
	
	
	public void createExcelFile(List<String[]> relationships, String userId, String fileLocation) {

		SXSSFWorkbook workbook = new SXSSFWorkbook(1000);
		
		// we need to iterate and write the headers during the first time
		Font headerFont = workbook.createFont();
		headerFont.setBold(true);
		
		// font for headers
		CellStyle centerCellStyleBold = workbook.createCellStyle();
		centerCellStyleBold.setFont(headerFont);
		centerCellStyleBold.setAlignment(HorizontalAlignment.CENTER);
		centerCellStyleBold.setVerticalAlignment(VerticalAlignment.CENTER);
		
		// font for normal cells
		CellStyle centerCellStyle = workbook.createCellStyle();
		centerCellStyle.setAlignment(HorizontalAlignment.CENTER);
		centerCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);

		SXSSFSheet sheet = workbook.createSheet("Relationships");
		sheet.setRandomAccessWindowSize(100);
		
		Cell cell;
		// Add Headers
		int excelRowCounter = 0;
		Row contractHeaders = sheet.createRow(excelRowCounter++);
		String[] headers = {START_TABLE, START_COLUMN, TARGET_TABLE, TARGET_COLUMN};
		for (int i = 0; i < headers.length; i++) {
			cell = contractHeaders.createCell(i);
			cell.setCellValue(headers[i]);
			cell.setCellStyle(centerCellStyleBold);
		}
		
		// Add the relationships
		for(String[] ship: relationships) {
			// starting string looks like this: 
			// http://semoss.org/ontologies/Relation/USER_SETTINGS.USER.STATION_SETTINGS.USER
			String[] splitPath = ship[2].split("/");
			String endOfPath = splitPath[splitPath.length - 1];
			
			// looks like this
			// [USER_SETTINGS, USER, STATION_SETTINGS, USER]
			String[] tablesAndColumns = endOfPath.split("\\.");

			Row relationship = sheet.createRow(excelRowCounter++);
			cell = relationship.createCell(0);
			cell.setCellValue(tablesAndColumns[0]);
			cell.setCellStyle(centerCellStyle);
			
			cell = relationship.createCell(1);
			cell.setCellValue(tablesAndColumns[1]);
			cell.setCellStyle(centerCellStyle);
			
			cell = relationship.createCell(2);
			cell.setCellValue(tablesAndColumns[2]);
			cell.setCellStyle(centerCellStyle);
			
			cell = relationship.createCell(3);
			cell.setCellValue(tablesAndColumns[3]);
			cell.setCellStyle(centerCellStyle);
		}

		// column width
		for(int colIndex = 0; colIndex < 10; colIndex++) {
			sheet.setColumnWidth(colIndex, 6_000);
		}
		
		// A second sheet with todays date and user ID
		sheet = workbook.createSheet("Date");
		sheet.setRandomAccessWindowSize(100);
		
		// Add a row with date
		excelRowCounter = 0;
		Row contractDetails = sheet.createRow(excelRowCounter++);
		cell = contractDetails.createCell(0);
		cell.setCellValue("Todays Date:");
		cell.setCellStyle(centerCellStyleBold);
		
		User user = ThreadStore.getUser();
		TimeZone tz = user.getTimeZone();
		SemossDate sDate = new SemossDate(ZonedDateTime.now(tz.toZoneId()).toLocalDateTime());
		cell = contractDetails.createCell(1);
		cell.setCellValue(sDate.getFormatted("yyyy-MM-dd HH-mm-ss"));
		cell.setCellStyle(centerCellStyle);
		
		cell = contractDetails.createCell(2);
		cell.setCellValue("User ID:");
		cell.setCellStyle(centerCellStyleBold);
		
		cell = contractDetails.createCell(3);
		cell.setCellValue(userId);
		cell.setCellStyle(centerCellStyle);
		
		// column width
		for(int colIndex = 0; colIndex < 10; colIndex++) {
			sheet.setColumnWidth(colIndex, 6_000);
		}
		
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if(password != null) {
			// encrypt file
			ExcelUtility.encrypt(workbook, fileLocation, password);
		} else {
			// write file
			ExcelUtility.writeToFile(workbook, fileLocation);
		}
	}	

	protected String getExportFileName(String fileNamePrefix, String extension) {
		// get a random file name
		User user = ThreadStore.getUser();
		TimeZone tz = user.getTimeZone();
		SemossDate sDate = new SemossDate(ZonedDateTime.now(tz.toZoneId()).toLocalDateTime());
		String dateFormatted = sDate.getFormatted("yyyy-MM-dd HH-mm-ss");
		String exportName = fileNamePrefix + "__" + dateFormatted + "." + extension;
		return exportName;
	}
}
