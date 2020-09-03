package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
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
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ToExcelReactor extends TaskBuilderReactor {

	private static final String CLASS_NAME = ToExcelReactor.class.getName();
	
	protected String fileLocation = null;
	protected Logger logger;
	protected boolean includeLogo = true;
	
	public ToExcelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.logger = getLogger(CLASS_NAME);
		this.task = getTask();
		NounMetadata retNoun = null;
		// get a random file name
		String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
		String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "xlsx");
		// grab file path to write the file
		this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (this.fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			{
				File f = new File(insightFolder);
				if(!f.exists()) {
					f.mkdirs();
				}
			}
			this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(exportName, this.fileLocation);
			retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} else {
			retNoun = new NounMetadata(this.fileLocation, PixelDataType.CONST_STRING);
		}
		buildTask();
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the excel file"));
		return retNoun;
	}
	
	@Override
	protected void buildTask() {
		SXSSFWorkbook workbook = new SXSSFWorkbook(1000);
		CreationHelper createHelper = workbook.getCreationHelper();
		SXSSFSheet sheet = workbook.createSheet("Results");
		sheet.setRandomAccessWindowSize(100);
		// freeze the first row
		sheet.createFreezePane(0, 1);
		
		int i = 0;
		int size = 0;
		// create typesArr as an array for faster searching
		String[] headers = null;
		SemossDataType[] typesArr = null;
		String[] additionalType = null;
		
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
			additionalType = new String[size];
			for(; i < size; i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
				typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
				additionalType[i] = headerInfo.get(i).get("additionalType") + "";
			}

			// generate the data row
			excelRow = sheet.createRow(++excelRowCounter);
			Object[] dataRow = row.getValues();
			i = 0;
			for(; i < size; i ++) {
				Cell cell = excelRow.createCell(i);
				Object value = dataRow[i];
				if(value == null) {
					cell.setCellValue("");
				} else {
					if(typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if(typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue( ((Number) value).doubleValue() ) ;
					} else if(typesArr[i] == SemossDataType.DATE) {
						cell.setCellValue( ((SemossDate) value).getDate() ) ;
						cell.setCellStyle(dateCellStyle);
					} else if(typesArr[i] == SemossDataType.TIMESTAMP) {
						cell.setCellValue( ((SemossDate) value).getDate() ) ;
						cell.setCellStyle(timeStampCellStyle);
					} else if(typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue( (boolean) value);
					} else {
						cell.setCellValue(value + "");
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
				if(value == null) {
					cell.setCellValue("");
				} else {
					if(typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if(typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue( ((Number) value).doubleValue() ) ;
					} else if(typesArr[i] == SemossDataType.DATE) {
						cell.setCellValue( ((SemossDate) value).getDate() ) ;
						cell.setCellStyle(dateCellStyle);
					} else if(typesArr[i] == SemossDataType.TIMESTAMP) {
						cell.setCellValue( ((SemossDate) value).getDate() ) ;
						cell.setCellStyle(timeStampCellStyle);
					} else if(typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue( (boolean) value);
					} else {
						cell.setCellValue(value + "");
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
}
