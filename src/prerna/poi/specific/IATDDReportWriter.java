package prerna.poi.specific;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class IATDDReportWriter {
	
	static final Logger logger = LogManager.getLogger(IndividualSystemTransitionReportWriter.class.getName());
	private XSSFWorkbook wb;
	private String selectedParam = "";
	private static String fileLoc = "";
	private static String templateLoc = "";

	public Hashtable<String,XSSFCellStyle> myStyles;
	
	public IATDDReportWriter(){}
	
	/**
	 * Creates a new workbook, sets the file location, and creates the styles to be used.
	 * @param systemName		String containing the name of the system to create the report for
	 */
	public void makeWorkbook(String selectedParam, String templateName) {
		this.selectedParam = selectedParam;
		
		fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\";
		templateLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\" + templateName;

		fileLoc += selectedParam + "_Catalog_Report.xlsx";
		wb = new XSSFWorkbook();

		//if a report template exists, then create a copy of the template, otherwise create a new workbook
		if(templateLoc!=null)
			try{
				wb = (XSSFWorkbook) WorkbookFactory.create(new File(templateLoc));
			}
		catch(Exception e){
			wb = new XSSFWorkbook();
		}

		makeStyles(wb);
	}

	/**
	 * Writes the workbook to the file location.
	 */
	public boolean writeWorkbook() {
		boolean success = false;
		try {
			wb.setForceFormulaRecalculation(true);
			Utility.writeWorkbook(wb, fileLoc);
			success = true;
		} catch (Exception ex) {
			success = false;
			ex.printStackTrace();
		}
		return success;
	}

	public static String getFileLoc() {
		return fileLoc;
	}
	
	public void writeCatalogSheet(String sheetName, HashMap<String,Object> result, String[] headers) {
		XSSFSheet sheetToWriteOver = wb.getSheet(sheetName);
		ArrayList<Object[]> dataList = (ArrayList<Object[]>) result.get("data");
		
		for (int row = 0; row < dataList.size(); row++) {
			Object[] resultRowValues = dataList.get(row);
			
			XSSFRow rowToWriteOn = sheetToWriteOver.getRow(row + 1);

			for (int col = 0; col < resultRowValues.length; col++) {
				XSSFCell cellToWriteOn = rowToWriteOn.getCell(col);
				if(resultRowValues[col] instanceof String) {
					String stringToWrite = ((String)resultRowValues[col]).replaceAll("\"", "").replaceAll("_"," ").replaceAll("~","/");
					if(stringToWrite.length()==0||stringToWrite.equals("NA"))
						stringToWrite = "TBD";
					cellToWriteOn.setCellValue(stringToWrite);
				}
			}			
		}
	    
	    wb.setPrintArea(0, 0, 3, 0, dataList.size());
	}
		
	/**
	 * Creates a cell border style in an Excel workbook
	 * @param wb 		Workbook to create the style
	 * @return style	XSSFCellStyle containing the format
	 */
	private static XSSFCellStyle createBorderedStyle(Workbook wb) {
		XSSFCellStyle style = (XSSFCellStyle)wb.createCellStyle();
		style.setBorderRight(CellStyle.BORDER_THIN);
		style.setRightBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderBottom(CellStyle.BORDER_THIN);
		style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderLeft(CellStyle.BORDER_THIN);
		style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
		style.setBorderTop(CellStyle.BORDER_THIN);
		style.setTopBorderColor(IndexedColors.BLACK.getIndex());
		return style;
	}

	/**
	 * Creates a cell format style for an excel workbook
	 * @param workbook 	XSSFWorkbook to create the format
	 */
	private void makeStyles(XSSFWorkbook workbook) {
		myStyles = new Hashtable<String,XSSFCellStyle>();
		XSSFCellStyle headerStyle = createBorderedStyle(workbook);
		Font boldFont = workbook.createFont();
		boldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		boldFont.setColor(IndexedColors.WHITE.getIndex());
		boldFont.setFontHeightInPoints((short) 10);
		headerStyle.setFont(boldFont);
		headerStyle.setAlignment(CellStyle.ALIGN_CENTER);
		headerStyle.setVerticalAlignment(CellStyle.VERTICAL_TOP);
		headerStyle.setFillForegroundColor(new XSSFColor(new java.awt.Color(54, 96, 146)));
		headerStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);
		myStyles.put("headerStyle",headerStyle);

		Font normalFont = workbook.createFont();
		normalFont.setFontHeightInPoints((short) 10);

		XSSFCellStyle normalStyle = createBorderedStyle(workbook);
		normalStyle.setWrapText(true);
		normalStyle.setFont(normalFont);
		normalStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("normalStyle",normalStyle);

		Font boldBodyFont = workbook.createFont();
		boldBodyFont.setFontHeightInPoints((short) 10);
		boldBodyFont.setBoldweight(Font.BOLDWEIGHT_BOLD);

		XSSFCellStyle boldStyle = createBorderedStyle(workbook);
		boldStyle.setWrapText(true);
		boldStyle.setFont(boldBodyFont);
		boldStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		myStyles.put("boldStyle",boldStyle);
		
		XSSFCellStyle accountStyle = createBorderedStyle(workbook);
		accountStyle.setFont(boldBodyFont);
		accountStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		XSSFDataFormat df = wb.createDataFormat();
		accountStyle.setDataFormat(df.getFormat("$* #,##0.00"));
		myStyles.put("accountStyle", accountStyle);
	}

}
