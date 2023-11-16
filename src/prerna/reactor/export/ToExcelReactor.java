package prerna.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.reactor.AbstractReactor;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ToExcelReactor extends TaskBuilderReactor {

	private static final Logger classLogger = LogManager.getLogger(ToExcelReactor.class);

	private static final String CLASS_NAME = ToExcelReactor.class.getName();
	
	protected String fileLocation = null;
	protected Logger logger;
	protected boolean includeLogo = true;
	
	public ToExcelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		this.logger = getLogger(CLASS_NAME);
		this.task = getTask();
		
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "xlsx");
		// grab file path to write the file
		this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (this.fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			File f = new File(insightFolder);
			if(!f.exists()) {
				f.mkdirs();
			}
			this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			this.fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(this.fileLocation);
		try {
			buildTask();
		} finally {
			if(this.task != null) {
				try {
					this.task.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the excel file"));
		return retNoun;
	}
	
	@Override
	protected void buildTask() {
		SXSSFWorkbook workbook = new SXSSFWorkbook(1000);
		CreationHelper createHelper = workbook.getCreationHelper();
		String sheetName = "Result";

		// get the panel
		InsightPanel panel = getInsightPanel();
		Map<String, Map<String, String>> panelFormatting = new HashMap<>();
		// if panel is passed 
		// use that for panel level formatting
		// and for the sheet name
		if(panel != null) {
			// panel level formatting
			panelFormatting = panel.getPanelFormatValues();
			// sheet name
			String sheetId = panel.getSheetId();
			InsightSheet sheet = this.insight.getInsightSheet(sheetId);
			sheetName = sheet.getSheetLabel();
			if (sheetName == null) {
				// since we are 0 based, add 1
				try {
					sheetName = "Sheet" + (Integer.parseInt(sheetId) + 1);
				} catch (Exception ignore) {
					sheetName = "Sheet " + sheetId;
				}
			}
		}
		
		SXSSFSheet sheet = workbook.createSheet(sheetName);
		sheet.setRandomAccessWindowSize(100);
		// freeze the first row
		sheet.createFreezePane(0, 1);
		
		int i = 0;
		int size = 0;
		// create typesArr as an array for faster searching
		String[] headers = null;
		SemossDataType[] typesArr = null;
		String[] additionalDataTypeArr = null;
		CellStyle[] stylingArr = null;
		
		// style dates
		CellStyle dateCellStyle = workbook.createCellStyle();
        dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));
        // style timestamps
        CellStyle timeStampCellStyle = workbook.createCellStyle();
        timeStampCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy HH:mm:ss"));
        
		// the excel data row
		Row excelRow = null;
		int excelRowCounter = 0;
		
		// we need to iterate and write the headers during the first time
		if(this.task.hasNext()) {
			IHeadersDataRow row = this.task.next();
			List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();
			
			// create the header row
	        Row headerRow = sheet.createRow(excelRowCounter);
			// create a Font for styling header cells
			Font headerFont = workbook.createFont();
			headerFont.setBold(true);
			// create a CellStyle with the font
			CellStyle headerCellStyle = workbook.createCellStyle();
			headerCellStyle.setFont(headerFont);
	        headerCellStyle.setAlignment(HorizontalAlignment.CENTER);
	        headerCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);

			// generate the header row
			// and define constants used throughout like size, and types
			i = 0;
			headers = row.getHeaders();
			size = headers.length;
			typesArr = new SemossDataType[size];
			additionalDataTypeArr = new String[size];
			stylingArr = new CellStyle[size];
			for(; i < size; i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
				// grab metadata from iterator
				typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
				additionalDataTypeArr[i] = headerInfo.get(i).get("additionalDataType") + "";
				try {
					stylingArr[i] = POIExportUtility.getCurrentStyle(workbook, additionalDataTypeArr[i], panelFormatting.get(headers[i]));
				} catch(Exception e) {
					// ignore
				}
				if(stylingArr[i] == null) {
					if(typesArr[i] == SemossDataType.DATE) {
						stylingArr[i] = dateCellStyle;
					} else if(typesArr[i] == SemossDataType.TIMESTAMP) {
						stylingArr[i] = timeStampCellStyle;
					}
				}
			}

			// generate the data row
			excelRow = sheet.createRow(++excelRowCounter);
			Object[] dataRow = row.getValues();
			i = 0;
			for(; i < size; i ++) {
				Cell cell = excelRow.createCell(i);
				Object value = dataRow[i];
				if(Utility.isNullValue(value)) {
					cell.setCellValue("null");
				} else {
					if(typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if(typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue( ((Number) value).doubleValue() ) ;
					} else if(typesArr[i] == SemossDataType.DATE) {
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if(typesArr[i] == SemossDataType.TIMESTAMP) {
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if(typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue( (boolean) value);
					} else {
						cell.setCellValue(value + "");
					}
					
					if(stylingArr[i] != null) {
						cell.setCellStyle(stylingArr[i]);
					}
				}
			}
		}
		
		// now iterate through all the data
		while(this.task.hasNext()) {
			excelRow = sheet.createRow(++excelRowCounter);
			IHeadersDataRow row = this.task.next();
			Object[] dataRow = row.getValues();
			i = 0;
			for(; i < size; i ++) {
				Cell cell = excelRow.createCell(i);
				Object value = dataRow[i];
				if(Utility.isNullValue(value)) {
					cell.setCellValue("null");
				} else {
					if(typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if(typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue( ((Number) value).doubleValue() ) ;
					} else if(typesArr[i] == SemossDataType.DATE) {
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if(typesArr[i] == SemossDataType.TIMESTAMP) {
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if(typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue( (boolean) value);
					} else {
						cell.setCellValue(value + "");
					}
					
					if(stylingArr[i] != null) {
						cell.setCellStyle(stylingArr[i]);
					}
				}
			}
		}
		
		// fixed size at the end
		i = 0;
		for(; i < size; i++) {
			sheet.setColumnWidth(i, 5_000);
		}
		
		if(includeLogo) {
			addLogo(workbook, sheet, size + 1);
		}
		
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if(password != null) {
			// encrypt file
			ExcelUtility.encrypt(workbook, this.fileLocation, password);
		} else {
			// write file
			ExcelUtility.writeToFile(workbook, this.fileLocation);
		}
	}
	
	private void addLogo(SXSSFWorkbook workbook, SXSSFSheet sheet, int startCol) {
		String semossLogoPath = DIHelper.getInstance().getProperty("EXPORT_SEMOSS_LOGO");
		if (semossLogoPath != null) {
			File logo = new File(semossLogoPath);
			if (logo.exists()) {
				// Load image
				byte[] picture = null;
				try {
					picture = IOUtils.toByteArray(new FileInputStream(semossLogoPath));
				} catch (FileNotFoundException e) {
					logger.error(Constants.STACKTRACE, e);
				} catch (IOException ioe) {
					logger.error(Constants.STACKTRACE, ioe);
				}
				
				// Insert image into workbook
				int pictureIndex = workbook.addPicture(picture, Workbook.PICTURE_TYPE_PNG);
				
				// Helper returns an object that handles instantiating concrete classes
				CreationHelper helper = workbook.getCreationHelper();
				Drawing drawing = sheet.createDrawingPatriarch();
				// Create an anchor that is attached to the worksheet
				ClientAnchor anchor = helper.createClientAnchor();
				// Set locations of anchor
				anchor.setCol1(startCol);
				anchor.setRow1(1);
				anchor.setCol2(startCol + 6);
				anchor.setRow2(4);
				// Create the picture
				Picture pict = drawing.createPicture(anchor, pictureIndex);
			}
		}
	}
	
	/**
	 * Get the insight panel
	 * @return
	 */
	private InsightPanel getInsightPanel() {
		// passed in directly as panel
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.PANEL) {
				return (InsightPanel) noun.getValue();
			} else if(nounType == PixelDataType.PANEL_CLONE_MAP) {
				Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
				return cloneMap.get("clone");
			} else if(nounType == PixelDataType.COLUMN || nounType == PixelDataType.CONST_STRING) {
				String panelId = noun.getValue().toString();
				return this.insight.getInsightPanel(panelId);
			}
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (InsightPanel) panelNouns.get(0).getValue();
		}
		
		// see if string or column passed in
		List<String> strInputs = this.curRow.getAllStrValues();
		if(strInputs != null && !strInputs.isEmpty()) {
			for(String panelId : strInputs) {
				InsightPanel panel = this.insight.getInsightPanel(panelId);
				if(panel != null) {
					return panel;
				}
			}
		}
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return this.insight.getInsightPanel(strNouns.get(0).getValue().toString());
		}
		
		// see if a clone map was passed
		genericReactorGrs = this.store.getNoun(PixelDataType.PANEL_CLONE_MAP.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
			return cloneMap.get("clone");
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL_CLONE_MAP);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
			return cloneMap.get("clone");
		}
		
		// well, you are out of luck
		return null;
	}
}
