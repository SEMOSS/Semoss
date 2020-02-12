package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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

	public ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.URL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting to pdf...");
		organizeKeys();
		// location for pdf resources
		String insightFolder = this.insight.getInsightFolder();
		String htmlToParse = this.keyValue.get(this.keysToGet[0]);
		String feUrl = this.keyValue.get(this.keysToGet[2]);
		htmlToParse = Utility.decodeURIComponent(htmlToParse);
		String sessionId = ThreadStore.getSessionId();

		// Find semoss tags
		Document doc = Jsoup.parse(htmlToParse);
		Elements semossElements = doc.select("semoss");

		// Process all semoss tags
		int imageNum = 1;
		for (Element element : semossElements) {
			String url = element.attr("url");

			// Run headless chrome with semossTagUrl
			String imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
			ChromeDriverUtility.captureImage(feUrl, url, imagePath, sessionId);
			// Replace semoss tag with img tag
			element.tagName("img");
			// Replace url attribute with src attribute
			element.removeAttr("url");
			element.attr("src", "image" + imageNum + ".png");
			imageNum += 1;
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
			String exportName = AbstractExportTxtReactor.getExportFileName("pdf");
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
		File tempXhtml = new File(insightFolder + DIR_SEPARATOR + random + ".html");
		try {
			FileUtils.writeStringToFile(tempXhtml, doc.html());
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// Convert from xhtml to pdf
		try {
			OutputStream os = new FileOutputStream(fileLocation);
			PdfRendererBuilder pdfBuilder = new PdfRendererBuilder();
			pdfBuilder.useFastMode();
			pdfBuilder.useDefaultPageSize(11.0f, 8.5f, PageSizeUnits.INCHES);
			pdfBuilder.withFile(tempXhtml);
			pdfBuilder.toStream(os);
			pdfBuilder.run();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return retNoun;
	}
}
