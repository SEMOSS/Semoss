package prerna.reactor.export;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.sl.usermodel.PictureData.PictureType;
import org.apache.poi.util.Units;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFPictureData;
import org.apache.poi.xslf.usermodel.XSLFPictureShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ToPPTReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ToPPTReactor.class);

	private static final String CLASS_NAME = ToPPTReactor.class.getName();

	public ToPPTReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.BASE_URL.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		String insightFolder = this.insight.getInsightFolder();
		String baseUrl = this.keyValue.get(this.keysToGet[0]);
		Integer waitTime = null;
		String waitTimeStr = this.keyValue.get(this.keysToGet[4]);
		if(waitTimeStr != null && (waitTimeStr=waitTimeStr.trim()).isEmpty()) {
			try {
				waitTime = Integer.parseInt(waitTimeStr);
			} catch(NumberFormatException e) {
				throw new IllegalArgumentException("Invalid wait time option = '" + waitTimeStr + "'. Error is: " + e.getMessage());
			}
		}
		
		List<String> urls = getUrls();

		String sessionId = ThreadStore.getSessionId();
		// keep list of paths to clean up and delete once the pdf is created
		List<String> tempPaths = new ArrayList<>();
		// Process all urls
		int imageNum = 1;
		for (String url : urls) {
			// Run headless chrome with semossTagUrl
			String imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
			logger.info("Generating image for PPT...");
			this.insight.getChromeDriver().captureImage(baseUrl, url, imagePath, sessionId, waitTime);
			tempPaths.add(imagePath);
			logger.info("Done generating image for PPT...");
			imageNum += 1;
		}

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		// grab file path to write the file
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "pptx");
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(fileLocation);

		// Insert images into powerpoint
		XMLSlideShow slideshow = new XMLSlideShow();
		for (String path : tempPaths) {
			byte[] pic = null;
			try {
				pic = IOUtils.toByteArray(new FileInputStream(Utility.normalizePath(path)));
			} catch (FileNotFoundException e) {
				logger.error(Constants.STACKTRACE, e);
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
			XSLFPictureData picData = slideshow.addPicture(pic, PictureType.PNG);
			Rectangle picBounds = createStandardPowerPointImageBounds();

			XSLFSlide slide = slideshow.createSlide();
			XSLFPictureShape pictureShape = slide.createPicture(picData);
			pictureShape.setAnchor(picBounds);
		}

		// Delete temp files
		for (String path : tempPaths) {
			try {
				File f = new File(Utility.normalizePath(path));
				if (f.exists()) {
					FileUtils.forceDelete(f);
				}
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		writeToFile(slideshow, fileLocation);

		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	private List<String> getUrls() {
		GenRowStruct colGrs = this.store.getNoun(keysToGet[1]);
		int size = colGrs.size();
		List<String> columns = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			columns.add(colGrs.get(i).toString());
		}
		return columns;
	}

	private Rectangle createStandardPowerPointImageBounds() {
		// Point DPI = 72 = 1 inch
		// Maintain 1920 x 1080 aspect ratio - let's fit to the width of the
		// slide
		double dpiPerInch = (double) Units.POINT_DPI;
		double slideWidth = 10;
		double slideHeight = 7.5;
		double widthOffsetInches = 0.1;
		double widthOffsetDPI = widthOffsetInches * dpiPerInch;
		double heightWidthAspectRatio = 1080.0 / 1920.0;

		double imageWidthDPI = ((slideWidth * dpiPerInch) - (2 * widthOffsetDPI));
		double imageHeightDPI = (imageWidthDPI * heightWidthAspectRatio);

		// Figure out height offset so that the image is in the middle of the
		// slide
		double slideHeightDPI = slideHeight * dpiPerInch;
		double heightOffsetDPI = ((slideHeightDPI - imageHeightDPI) / 2);

		// Cast coordinates to int so that they can be ingested by Rectangle
		int widthOffsetDPIInt = (int) widthOffsetDPI;
		int heightOffsetDPIInt = (int) heightOffsetDPI;
		int imageWidthDPIInt = (int) imageWidthDPI;
		int imageHeightDPIInt = (int) imageHeightDPI;

		return new java.awt.Rectangle(widthOffsetDPIInt, heightOffsetDPIInt, imageWidthDPIInt, imageHeightDPIInt);
	}

	private void writeToFile(XMLSlideShow slideshow, String path) {
		try {
			OutputStream out = new FileOutputStream(path);
			slideshow.write(out);
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		}
	}
}
