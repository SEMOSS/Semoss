package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.openhtmltopdf.outputdevice.helper.BaseRendererBuilder.PageSizeUnits;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;

import prerna.om.ThreadStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ChromeDriverUtility;
import prerna.util.Utility;

public class ToPdfReactor extends AbstractReactor {

	private static final String CLASS_NAME = ToPdfReactor.class.getName();
	private static final String STACKTRACE = "StackTrace: ";

	public ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.URL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		// location for pdf resources
		String insightFolder = this.insight.getInsightFolder();
		String htmlToParse = this.keyValue.get(ReactorKeysEnum.HTML.getKey());
		String feUrl = this.keyValue.get(ReactorKeysEnum.URL.getKey());
		htmlToParse = Utility.decodeURIComponent(htmlToParse);
		String sessionId = ThreadStore.getSessionId();

		// Find semoss tags
		Document doc = Jsoup.parse(htmlToParse);
		Elements semossElements = doc.select("semoss");
		// keep list of paths to clean up and delete once the pdf is created
		Vector<String> tempPaths = new Vector<>();
		// Process all semoss tags
		int imageNum = 1;
		for (Element element : semossElements) {
			String url = element.attr("url");

			// Run headless chrome with semossTagUrl
			String imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
			while(new File(imagePath).exists()) {
				imageNum++;
				imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
			}
			logger.info("Generating image for PDF...");
			ChromeDriverUtility.captureImage(feUrl, url, imagePath, sessionId);
			tempPaths.add(imagePath);
			logger.info("Done generating image for PDF...");

			// Replace semoss tag with img tag
			element.tagName("img");
			// Replace url attribute with src attribute
			element.removeAttr("url");
			element.attr("src", "image" + imageNum + ".png");
			imageNum++;
		}

		// Convert from html to xhtml
		doc.outputSettings().syntax(Document.OutputSettings.Syntax.xml);

		// get a random file name
		// grab file path to write the file
		NounMetadata retNoun = null;
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
			String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "pdf");
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(exportName, fileLocation);
			retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} else {
			retNoun = new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
		}

		// Flush xhtml to disk
		String random = Utility.getRandomString(5);
		String tempXhtmlPath = insightFolder + DIR_SEPARATOR + random + ".html";
		File tempXhtml = new File(tempXhtmlPath);
		try {
			FileUtils.writeStringToFile(tempXhtml, doc.html());
			tempPaths.add(tempXhtmlPath);
		} catch (IOException e1) {
			logger.error(STACKTRACE, e1);
		}

		// Convert from xhtml to pdf
		FileOutputStream fos = null;
		try {
			logger.info("Converting html to PDF...");
			fos = new FileOutputStream(fileLocation);
			PdfRendererBuilder pdfBuilder = new PdfRendererBuilder();
			pdfBuilder.useFastMode();
			pdfBuilder.useDefaultPageSize(11.0f, 8.5f, PageSizeUnits.INCHES);
			pdfBuilder.withFile(tempXhtml);
			pdfBuilder.toStream(fos);
			pdfBuilder.run();
			logger.info("Done converting html to PDF...");
		} catch (FileNotFoundException e) {
			logger.error(STACKTRACE, e);
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		} catch (Exception ex) {
			logger.error(STACKTRACE, ex);
		} finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// delete temp files
		for (String path : tempPaths) {
			try {
				File f = new File(Utility.normalizePath(path));
				if (f.exists()) {
					FileUtils.forceDelete(f);
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}

		return retNoun;
	}
}
