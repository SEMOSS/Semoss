package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xhtmlrenderer.pdf.ITextRenderer;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.export.mustache.MustacheUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ToPdfReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToPdfReactor.class);
	private static final String CLASS_NAME = ToPdfReactor.class.getName();

	private static final String MUSTACHE = "mustache";
	private static final String VARMAP = "varMap";
	
	public ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.URL.getKey(),
				MUSTACHE, VARMAP};
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
		htmlToParse = Utility.decodeURIComponent(htmlToParse);
		// see if using mustache template format that needs modifications
		if(Boolean.parseBoolean(this.keyValue.get(MUSTACHE) + "")) {
			Map<String, Object> variables = mustacheVariables();
			try {
				htmlToParse = MustacheUtility.compile(htmlToParse, variables);
			} catch (Exception e) {
				throw new IllegalArgumentException("Invalid mustache template or variables. Detailed error message = " + e.getMessage(), e);
			}
			classLogger.info("Exporting final html as: " + htmlToParse);
		}
		// keep track for deleting at the end
		List<String> tempPaths = new ArrayList<>();

		// Find semoss tags
		Document doc = Jsoup.parse(htmlToParse);
		Elements semossElements = doc.select("semoss");
		if(!semossElements.isEmpty()) {
			String feUrl = this.keyValue.get(ReactorKeysEnum.URL.getKey());
			if(feUrl == null || feUrl.isEmpty()) {
				throw new IllegalArgumentException("Must pass in the URL for the UI");
			}
			String sessionId = ThreadStore.getSessionId();

			// keep list of paths to clean up and delete once the pdf is created
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
			ITextRenderer renderer = new ITextRenderer();
	        renderer.setDocument(tempXhtml.getAbsoluteFile());
	        renderer.layout();
	        renderer.createPDF(fos);
	        fos.close();
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
	
	private Map<String, Object> mustacheVariables() {
		GenRowStruct grs = this.store.getNoun(VARMAP);
		if(grs != null && !grs.isEmpty()) {
			Object obj = grs.get(0);
			if(!(obj instanceof Map)) {
				throw new IllegalArgumentException(VARMAP + " must be a map object");
			}
			return (Map<String, Object>) obj;
		}
		
		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}
		
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(MUSTACHE)) {
			return "Boolean if the html passed in is a mustache template";
		} else if(key.equals(VARMAP)) {
			return "Map containing the replacement values for a mustache tempalte. Must pass in mustache=true for this to be utilized";
		}
		
		return super.getDescriptionForKey(key);
	}
}
