package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.openhtmltopdf.outputdevice.helper.BaseRendererBuilder.PageSizeUnits;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class ToPdfReactor extends AbstractReactor {

	private static final String CLASS_NAME = ToPdfReactor.class.getName();

	public ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.URL.getKey() };
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
			this.insight.getChromeDriver().captureImage(feUrl, url, imagePath, sessionId);
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

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "pdf");
		// grab file path to write the file
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(fileLocation);

		// Flush xhtml to disk
		String random = Utility.getRandomString(5);
		String tempXhtmlPath = insightFolder + DIR_SEPARATOR + random + ".html";
		File tempXhtml = new File(tempXhtmlPath);
		try {
			FileUtils.writeStringToFile(tempXhtml, doc.html());
			tempPaths.add(tempXhtmlPath);
		} catch (IOException e1) {
			logger.error(Constants.STACKTRACE, e1);
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
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
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
				logger.error(Constants.STACKTRACE, e);
			}
		}

		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);
		
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the tsv file"));
		return retNoun;
	}
}
