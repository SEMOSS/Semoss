package prerna.reactor.export;

import java.awt.Color;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.poi.hslf.usermodel.HSLFPictureData;
import org.apache.poi.sl.usermodel.PictureData;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFPictureData;
import org.apache.poi.xslf.usermodel.XSLFPictureShape;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFSlideLayout;
import org.apache.poi.xslf.usermodel.XSLFSlideMaster;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextRun;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import org.openqa.selenium.chrome.ChromeDriver;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ChromeDriverUtility;
import prerna.util.Utility;

// export to excel non-native is the NN
public class ExportToPPTNNReactor extends AbstractReactor {

	public static final String exportTemplate = "PPT_EXPORT_TEMPLATE";


	public ExportToPPTNNReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.USE_PANEL.getKey(), ReactorKeysEnum.HEIGHT.getKey(), ReactorKeysEnum.WIDTH.getKey(), ReactorKeysEnum.SLIDE_LAYOUT.getKey(), ReactorKeysEnum.SHAPE_INDEX.getKey(), ReactorKeysEnum.EXPORT_TEMPLATE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the number of sheets
		// export each sheet using the insight definition
		// Open excel
		// embed each of the sheet
		// need to introduce width and height
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		
		String insightFolder = this.insight.getInsightFolder();
		String fileName = null;
		int height = 800;
		int width = 600;
		String slideLayout = null;
		int shapeIndex = -1;

		if(keyValue.containsKey(ReactorKeysEnum.FILE_PATH.getKey())) {
			insightFolder =  Utility.normalizePath((String)keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
			insightFile.setDeleteOnInsightClose(false);
		}
		if(keyValue.containsKey(ReactorKeysEnum.FILE_NAME.getKey())) {
			fileName =  Utility.normalizePath((String)keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		}

		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.insight.getLiveURL();
		boolean panel = false;
		if(keyValue.containsKey(ReactorKeysEnum.USE_PANEL.getKey())) {
			String panelUse= (String)keyValue.get(ReactorKeysEnum.USE_PANEL.getKey());
			panel = panelUse.equalsIgnoreCase("yes") || panelUse.equalsIgnoreCase("true");
		}

		if(keyValue.containsKey(ReactorKeysEnum.HEIGHT.getKey())) {
			height= Integer.parseInt(keyValue.get(ReactorKeysEnum.HEIGHT.getKey()));
		}

		if(keyValue.containsKey(ReactorKeysEnum.WIDTH.getKey())) {
			width= Integer.parseInt(keyValue.get(ReactorKeysEnum.WIDTH.getKey()));
		}

		if(keyValue.containsKey(ReactorKeysEnum.SLIDE_LAYOUT.getKey())) {
			slideLayout= keyValue.get(ReactorKeysEnum.SLIDE_LAYOUT.getKey());
		}

		if(keyValue.containsKey(ReactorKeysEnum.SHAPE_INDEX.getKey())) {
			shapeIndex= Integer.parseInt(keyValue.get(ReactorKeysEnum.SHAPE_INDEX.getKey()));
		}

		String template = insight.getProperty(exportTemplate);
		Map <String, InsightSheet> allSheets = insight.getInsightSheets();
		Map <String, InsightPanel> allPanels = insight.getInsightPanels();

		Iterator <String> keys = allSheets.keySet().iterator();
		if(panel) {
			keys = allPanels.keySet().iterator();
		}
		
		// open a workbook
		XMLSlideShow hslfSlideShow = null;
		FileOutputStream fileOut = null;
		Object driver = null;
		ChromeDriverUtility util = null;
		XSLFSlideLayout targetLayout = null;
		try {
			if(template != null) {
				hslfSlideShow = new XMLSlideShow(new FileInputStream(template));	 
				if(slideLayout != null) {
					targetLayout = getLayout(hslfSlideShow, slideLayout);
				}
			} else {
				hslfSlideShow = new XMLSlideShow();	 
			}

			XSLFSlide templateSlide = null;
			if(template != null) {
				// assumes 0th slide is the slide
				templateSlide = hslfSlideShow.getSlides().get(0);
			}


			while(keys.hasNext()) {
				String thisKey = keys.next();
				String sheetAppender = "";
				String panelAppender = "";

				if(panel) {			   
					InsightPanel thisPanel = allPanels.get(thisKey);
					panelAppender = "&panel=" + thisKey;

					InsightSheet thisSheet = allSheets.get(thisPanel.getSheetId());
					sheetAppender = "&sheet=" + thisSheet.getSheetId();
				} else {
					InsightSheet thisSheet = allSheets.get(thisKey);
					sheetAppender = "&sheet=" + thisKey;
				}

				XSLFSlide blankSlide = null;

				// make a copy of the slide
				if(templateSlide != null && targetLayout != null) {
					blankSlide = hslfSlideShow.createSlide(targetLayout);
					// apply this layout
				} else if(templateSlide != null) {
					blankSlide = hslfSlideShow.createSlide();
					blankSlide.importContent(templateSlide);
				} else {
					blankSlide = hslfSlideShow.createSlide();
				}

				// now capture the image and fill it
				String prefixName = Utility.getRandomString(8);
				String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "png");
				String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
				util = new ChromeDriverUtility();

				if(driver == null) {
					driver = util.makeChromeDriver(baseUrl, imageUrl + sheetAppender + panelAppender, height, width);
				}
				// download this file
				util.captureImagePersistent(driver, baseUrl, imageUrl + sheetAppender + panelAppender, fileLocation, sessionId, 800);
				//driver = ChromeDriverUtility.captureImage(baseUrl, imageUrl + sheetAppender + panelAppender, fileLocation, sessionId, 800, 600, false);
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

				// see if the shape index is specified and if so place it there
				if(shapeIndex == -1) {
					XSLFPictureShape pic = blankSlide.createPicture(hslfPictureData);
					pic.setAnchor(new Rectangle(0, 0, height, width));
				} else {
					List<XSLFShape> shapes = blankSlide.getShapes();

					// get the anchor of the shape
					XSLFShape pic =  shapes.get(1);
					java.awt.geom.Rectangle2D anchor2 = pic.getAnchor();
					XSLFPictureData pd = hslfSlideShow.addPicture(bytes, PictureData.PictureType.PNG);

					XSLFPictureShape picture = blankSlide.createPicture(pd);
					blankSlide.removeShape(pic);
					picture.setAnchor(anchor2);  

					// may be fill in the title too
					// add title to the slide
					String title = allSheets.get(thisKey).getSheetLabel();
					XSLFShape titleShape = shapes.get(0);
					XSLFTextShape textShape = (XSLFTextShape) titleShape;
					textShape.clearText();
					XSLFTextParagraph p = textShape.addNewTextParagraph();
					XSLFTextRun r1 = p.addNewTextRun();
					r1.setText(title);
					r1.setFontColor(Color.blue);
					r1.setFontSize(48.);
				}
			}

			// remove the template slide
			if(templateSlide != null && slideLayout == null) {
				hslfSlideShow.removeSlide(0);
			}

			String prefixName = fileName;
			String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "pptx");
			String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			fileOut = new FileOutputStream(fileLocation);
			hslfSlideShow.write(fileOut);
			
			insightFile.setFilePath(fileLocation);
			// store the insight file 
			// in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(downloadKey, insightFile);

			NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the ppt file"));
			return retNoun;
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occurred generating the ppt file");
		} finally {
			if(fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(driver != null && driver instanceof ChromeDriver) {
				((ChromeDriver)driver).quit();
			}
		}
	}

	// get the layout to use
	public XSLFSlideLayout getLayout(XMLSlideShow ppt, String targetLayoutName) {
		XSLFSlideLayout targetLayout = null;

		// do the power point
		List <XSLFSlideMaster> sm = ppt.getSlideMasters();
		for(int slideMasterIndex = 0;slideMasterIndex < sm.size();slideMasterIndex++) {
			XSLFSlideMaster thisSM = sm.get(slideMasterIndex);
			XSLFSlideLayout [] sl = thisSM.getSlideLayouts();
			for(int layoutIndex = 0; layoutIndex < sl.length; layoutIndex++) {
				//if(targetLayout == null)
				//   targetLayout = sl[layoutIndex]; // assign to the first one - need some layout
				//if(targetLayoutName == null)
				//   break;
				if(sl[layoutIndex].getName().equalsIgnoreCase(targetLayoutName)) {
					targetLayout = sl[layoutIndex];
					break;
				}
			}
		}
		return targetLayout;
	}

}
