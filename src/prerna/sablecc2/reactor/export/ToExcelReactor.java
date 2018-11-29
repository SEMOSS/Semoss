package prerna.sablecc2.reactor.export;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
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
	
	public ToExcelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.logger = getLogger(CLASS_NAME);
		this.task = getTask();
		
		// get a random file name
		String randomKey = UUID.randomUUID().toString();
		this.fileLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + randomKey + ".xlsx";
		buildTask();
		
		// store it in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(randomKey, this.fileLocation);
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	@Override
	protected void buildTask() {
		Workbook workbook = new XSSFWorkbook();
		CreationHelper createHelper = workbook.getCreationHelper();
		Sheet sheet = workbook.createSheet("Results");
		// freeze the first row
		sheet.createFreezePane(0, 1);
		
		int i = 0;
		int size = 0;
		// create typesArr as an array for faster searching
		String[] headers = null;
		SemossDataType[] typesArr = null;
		
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
			for(; i < size; i++) {
				Cell cell = headerRow.createCell(i);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
				typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
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
		
		// Resize all columns to fit the content size
        for(i = 0; i < size; i++) {
            sheet.autoSizeColumn(i);
        }

        // Write the output to a file
        FileOutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(this.fileLocation);
			 workbook.write(fileOut);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(workbook != null) {
				try {
					workbook.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
