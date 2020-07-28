package prerna.sablecc2.reactor.export;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ChromeDriverUtility;
import prerna.util.Utility;


// export to excel non-native is the NN
public class ExportToExcelNNReactor extends AbstractReactor {

	public ExportToExcelNNReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.USE_PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		
		// get the number of sheets
		// export each sheet using the insight definition
		// Open excel
		// embed each of the sheet
		NounMetadata retNoun = null;
		organizeKeys();
		String insightFolder = this.insight.getInsightFolder();
		String fileName = null;
		
		if(keyValue.containsKey(ReactorKeysEnum.FILE_PATH.getKey()))
			insightFolder = (String)keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		if(keyValue.containsKey(ReactorKeysEnum.FILE_NAME.getKey()))
			fileName = (String)keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
		
		String baseUrl = this.insight.baseURL;
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.insight.getLiveURL();
		boolean panel = false;
		if(keyValue.containsKey(ReactorKeysEnum.USE_PANEL.getKey()))
		{
			String panelUse= (String)keyValue.get(ReactorKeysEnum.USE_PANEL.getKey());
			panel = panelUse.equalsIgnoreCase("yes") || panelUse.equalsIgnoreCase("true");
		}
		
		
		// open a workbook
	   Workbook wb = new XSSFWorkbook();
	   
	   Map <String, InsightSheet> allSheets = insight.getInsightSheets();
	   Map <String, InsightPanel> allPanels = insight.getInsightPanels();
	   
	   Iterator <String> keys = allSheets.keySet().iterator();
	   if(panel)
		   keys = allPanels.keySet().iterator();
	   
	   List <String> processedSheetPanel = new ArrayList<String>();
	   
	   try {
		   while(keys.hasNext())
		   {
			   String thisKey = keys.next();
			   String sheetAppender = "";
			   String panelAppender = "";
			   String sheetLabel = "";
			   String sheetKey = "";
			   
			   if(panel)
			   {			   
				   InsightPanel thisPanel = allPanels.get(thisKey);
				   panelAppender = "&panel=" + thisKey;
				   
				   InsightSheet thisSheet = allSheets.get(thisPanel.getSheetId());
				   sheetAppender = "&sheet=" + thisSheet.getSheetId();
				   sheetKey = thisSheet.getSheetId();
				   sheetLabel = thisSheet.getSheetLabel();
				   if(sheetLabel == null)
					   sheetLabel = "Sheet" + (Integer.parseInt(sheetKey) + 1); 
				   sheetLabel = sheetLabel + " Panel - " + thisKey;
			   }
			   else
			   {
				   InsightSheet thisSheet = allSheets.get(thisKey);
				   sheetAppender = "&sheet=" + thisKey;
				   sheetKey = thisKey;				   
				   sheetLabel = thisSheet.getSheetLabel();
				   if(sheetLabel == null)
					   sheetLabel = "Sheet" + (Integer.parseInt(thisKey) + 1); 
			   }
			   
			   Sheet sheet = wb.createSheet(sheetLabel);
			   
			   // now capture the image and fill it
				String prefixName = Utility.getRandomString(8);
				String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "png");
				String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			 
				// download this file
				ChromeDriverUtility.captureImage(baseUrl, imageUrl + sheetAppender + panelAppender, fileLocation, sessionId, 800, 600, true);
				// write this to the sheet now
				
				//1920 x 936
			   //FileInputStream obtains input bytes from the image file
			   InputStream inputStream = new FileInputStream(fileLocation);
			   //Get the contents of an InputStream as a byte[].
			   byte[] bytes = IOUtils.toByteArray(inputStream);
			   //Adds a picture to the workbook
			   int pictureIdx = wb.addPicture(bytes, Workbook.PICTURE_TYPE_PNG);
			   //close the input stream
			   inputStream.close();
			   //Returns an object that handles instantiating concrete classes
			   CreationHelper helper = wb.getCreationHelper();
			   //Creates the top-level drawing patriarch.
			   Drawing drawing = sheet.createDrawingPatriarch();

			   //Create an anchor that is attached to the worksheet
			   ClientAnchor anchor = helper.createClientAnchor();

			   //create an anchor with upper left cell _and_ bottom right cell
			   anchor.setCol1(1); //Column B
			   anchor.setRow1(2); //Row 3
			   anchor.setCol2(2); //Column C // doesnt matter
			   anchor.setRow2(4); //Row 4

			   //Creates a picture
			   Picture pict = drawing.createPicture(anchor, pictureIdx);
			   pict.resize();

			   //Reset the image to the original size
			   //pict.resize(); //don't do that. Let the anchor resize the image!
			   //Create the Cell B3
			   Cell cell = sheet.createRow(2).createCell(1);
		   }
		   String prefixName = fileName;
		   String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "xlsx");
		   String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
		   FileOutputStream fileOut = null;
		   fileOut = new FileOutputStream(fileLocation);
		   wb.write(fileOut);
		   fileOut.close();
		   retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		// now that the file location has been set
		// create the excel file there
		
		
		
		return retNoun;
	}
}
