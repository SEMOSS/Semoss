package prerna.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;

import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class DownloadFile extends AbstractReactor {

	public DownloadFile() {
		// keep open specifies whether to keep this open or close it. if kept open then
		// this will return open as noun metadata
		// footer can be anything
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FOOTER.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		String fileName = null;
		organizeKeys();
		String disclaimer = "";

		try {
			if (keyValue.containsKey(keysToGet[0])) {
				fileName =  Utility.normalizePath(keyValue.get(keysToGet[0]));
			}
			if (keyValue.containsKey(keysToGet[1])) {
				disclaimer = keyValue.get(keysToGet[1]);
			}
			Map<String, Object> exportMap = (Map) insight.getVar(insight.getInsightId());

			if(exportMap.containsKey(keysToGet[1])) {
				disclaimer = (String)exportMap.get(keysToGet[1]);
			}
			
			// get the workbook
			if(fileName == null) {
				fileName = (String)exportMap.get("FILE_NAME");
			}
			Workbook wb = (Workbook) exportMap.get(fileName);
			
			if(disclaimer != null && disclaimer.length() > 0) {
				fillFooters(wb, exportMap, disclaimer);
			}

			if(exportMap.containsKey("para1") || exportMap.containsKey("para2")) {
				fillHeaders(wb, exportMap, (String)exportMap.get("para1"), (String)exportMap.get("para2"));
			}
			
			String exportName = AbstractExportTxtReactor.getExportFileName(fileName, "xlsx");
			File file = new File(insight.getInsightFolder());
			if (!file.exists()) {
				file.mkdirs();
			}
			String fileLocation = insight.getInsightFolder() + DIR_SEPARATOR + exportName;

			FileOutputStream fileOut = new FileOutputStream(fileLocation);
			wb.write(fileOut);
			insight.getVarStore().remove(insight.getInsightId());
			
			// store the insight file 
			// in the insight so the FE can download it
			// only from the given insight
			String downloadKey = UUID.randomUUID().toString();
			InsightFile insightFile = new InsightFile();
			insightFile.setFilePath(fileLocation);
			insightFile.setDeleteOnInsightClose(true);
			insightFile.setFileKey(downloadKey);
			this.insight.addExportFile(downloadKey, insightFile);
			NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
			
			return retNoun;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	
	private void fillHeaders(Workbook wb, Map exportMap, String para1, String para2)
	{
		// usually the company name is in the first row 4th column
		for(int sheetIndex = 0;sheetIndex < wb.getNumberOfSheets();sheetIndex++)
		{
			Sheet aSheet = wb.getSheetAt(sheetIndex);
			String sheetName = aSheet.getSheetName();

			// should already be there but
			Row para1Row = aSheet.getRow(0);
			if(para1Row == null)
				para1Row = aSheet.createRow(0);
			Row para2Row = aSheet.getRow(1);
			if(para2Row == null)
				para2Row = aSheet.createRow(1);
			
			Cell para1Cell = para1Row.getCell(3);
			Cell para2Cell = para2Row.getCell(3);
			
			if(para1Cell == null) // ok weird
				para1Cell = para1Row.createCell(3);
			if(para2Cell == null) // ok weird
				para2Cell = para2Row.createCell(3);

			CellStyle style = wb.createCellStyle(); //Create new style
            style.setWrapText(true); //Set wordwrap
            if(para1 != null)
            	para1Cell.setCellValue(para1);
            if(para2 != null)
            	para2Cell.setCellValue(para2);				
		}
	}

	private void fillFooters(Workbook wb, Map exportMap, String footer)
	{
		
		for(int sheetIndex = 0;sheetIndex < wb.getNumberOfSheets();sheetIndex++)
		{
			Sheet aSheet = wb.getSheetAt(sheetIndex);
			String sheetName = aSheet.getSheetName();
			
			// final row count 
			int sheetTotalRows = 0;
			if(wb.getSheet("template") != null)
				sheetTotalRows = 5; // leave space foe headers
			if(exportMap.containsKey(sheetName + "ROW_COUNT"))
				sheetTotalRows = (Integer)exportMap.get(sheetName + "ROW_COUNT");
			
			Row row = aSheet.createRow(sheetTotalRows + 2);
			Cell cell = row.createCell(2);
			CellStyle style = wb.createCellStyle(); //Create new style
            style.setWrapText(true); //Set wordwrap
            cell.setCellStyle(style); //Apply style to cell
			cell.setCellValue(footer);
			
			aSheet.addMergedRegion(new CellRangeAddress(sheetTotalRows + 2, sheetTotalRows +4, 2, 20));			
		}
		wb.removeSheetAt(wb.getSheetIndex("template"));		
		
	}
	

}
