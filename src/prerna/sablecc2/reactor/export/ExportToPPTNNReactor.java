package prerna.sablecc2.reactor.export;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.poi.hslf.usermodel.HSLFPictureData;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFPictureData;
import org.apache.poi.xslf.usermodel.XSLFPictureShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;

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
public class ExportToPPTNNReactor extends AbstractReactor {
	
	public static final String exportTemplate = "EXCEL_EXPORT_TEMPLATE";
	

	public ExportToPPTNNReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.USE_PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		
		// get the number of sheets
		// export each sheet using the insight definition
		// Open excel
		// embed each of the sheet
		// need to introduce width and height
		NounMetadata retNoun = null;
		organizeKeys();
		String insightFolder = this.insight.getInsightFolder();
		String fileName = null;

		
		if(keyValue.containsKey(ReactorKeysEnum.FILE_PATH.getKey()))
			insightFolder = (String)keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		if(keyValue.containsKey(ReactorKeysEnum.FILE_NAME.getKey()))
			fileName = (String)keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
		
		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.insight.getLiveURL();
		boolean panel = false;
		if(keyValue.containsKey(ReactorKeysEnum.USE_PANEL.getKey()))
		{
			String panelUse= (String)keyValue.get(ReactorKeysEnum.USE_PANEL.getKey());
			panel = panelUse.equalsIgnoreCase("yes") || panelUse.equalsIgnoreCase("true");
		}
		
		String template = insight.getProperty(exportTemplate);

		// open a workbook
		XMLSlideShow hslfSlideShow = null;
	   
	   Map <String, InsightSheet> allSheets = insight.getInsightSheets();
	   Map <String, InsightPanel> allPanels = insight.getInsightPanels();
	   
	   Iterator <String> keys = allSheets.keySet().iterator();
	   if(panel)
		   keys = allPanels.keySet().iterator();
	   
	   
	   try {
			if(template != null)
				hslfSlideShow = new XMLSlideShow(new FileInputStream(exportTemplate));	 
			else
				hslfSlideShow = new XMLSlideShow();	 

			XSLFSlide templateSlide = null;
		   if(exportTemplate != null)
		   {
			   // assumes 0th slide is the slide
			   templateSlide = hslfSlideShow.getSlides().get(0);
		   }

			while(keys.hasNext())
		   {
			   String thisKey = keys.next();
			   String sheetAppender = "";
			   String panelAppender = "";
			   
			   if(panel)
			   {			   
				   InsightPanel thisPanel = allPanels.get(thisKey);
				   panelAppender = "&panel=" + thisKey;
				   
				   InsightSheet thisSheet = allSheets.get(thisPanel.getSheetId());
				   sheetAppender = "&sheet=" + thisSheet.getSheetId();
			   }
			   else
			   {
				   InsightSheet thisSheet = allSheets.get(thisKey);
				   sheetAppender = "&sheet=" + thisKey;
			   }
			   
			   XSLFSlide blankSlide = hslfSlideShow.createSlide();
			   // make a copy of the slide
			   if(templateSlide != null)
				   blankSlide.importContent(templateSlide);
			   
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
			   //close the input stream
			   inputStream.close();
			   
			   FileUtils.forceDelete(new File(fileLocation));

			   XSLFPictureData hslfPictureData = hslfSlideShow.addPicture(bytes, HSLFPictureData.PictureType.PNG);
			   XSLFPictureShape pic = blankSlide.createPicture(hslfPictureData);
			   pic.setAnchor(new Rectangle(0,0, 800,600));
			   			   
		   }
		   
		   String prefixName = fileName;
		   
		   // remove the template slide
		   if(templateSlide != null)
			   hslfSlideShow.removeSlide(0);
		   
		   String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "pptx");
		   String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
		   FileOutputStream fileOut = null;
		   fileOut = new FileOutputStream(fileLocation);
		   hslfSlideShow.write(fileOut);
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
