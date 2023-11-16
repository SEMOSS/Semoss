package prerna.reactor.export;

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
import org.apache.pdfbox.pdmodel.PDDocument;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xhtmlrenderer.pdf.ITextRenderer;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.mustache.MustacheUtility;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.reactor.export.pdf.PDFUtility.FormObject;
import prerna.reactor.export.pdf.PDFUtility.RectanglePage;
import prerna.reactor.export.pdf.PDFUtility.pageLocation;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class ToPdfReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToPdfReactor.class);
	private static final String CLASS_NAME = ToPdfReactor.class.getName();

	public ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.OUTPUT_FILE_PATH.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.URL.getKey(), 
				ReactorKeysEnum.MUSTACHE.getKey(), ReactorKeysEnum.MUSTACHE_VARMAP.getKey(), 
				ReactorKeysEnum.PDF_SIGNATURE_BLOCK.getKey(), ReactorKeysEnum.PDF_SIGNATURE_LABEL.getKey(),
				ReactorKeysEnum.PDF_PAGE_NUMBERS.getKey(), ReactorKeysEnum.PDF_PAGE_NUMBERS_IGNORE_FIRST.getKey(), ReactorKeysEnum.PDF_START_PAGE_NUM.getKey(),
				ReactorKeysEnum.IMAGE_WAIT_TIME.getKey()
			};
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
		if(htmlToParse == null || (htmlToParse=htmlToParse.trim()).isEmpty()) {
			// guessing its passed as a file
			String htmlFileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
			File htmlFile = new File(htmlFileLocation);
			if(!htmlFile.exists() || !htmlFile.isFile()) {
				throw new IllegalArgumentException("No html passed in directly and could not find input file");
			}
			try {
				htmlToParse = FileUtils.readFileToString(htmlFile, "UTF-8");
			} catch (IOException e) {
				throw new IllegalArgumentException("Error reading html file with message = " + e.getMessage(), e);
			}
		} else {
			htmlToParse = Utility.decodeURIComponent(htmlToParse);
		}
		// see if using mustache template format that needs modifications
		if(Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.MUSTACHE.getKey()) + "")) {
			Map<String, Object> variables = mustacheVariables();
			try {
				htmlToParse = MustacheUtility.compile(htmlToParse, variables);
			} catch (Exception e) {
				throw new IllegalArgumentException("Invalid mustache template or variables. Detailed error message = " + e.getMessage(), e);
			}
			classLogger.error("Exporting final html as: " + htmlToParse);
		}
		
		Integer waitTime = null;
		String waitTimeStr = this.keyValue.get(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey());
		if(waitTimeStr != null && (waitTimeStr=waitTimeStr.trim()).isEmpty()) {
			try {
				waitTime = Integer.parseInt(waitTimeStr);
			} catch(NumberFormatException e) {
				throw new IllegalArgumentException("Invalid wait time option = '" + waitTimeStr + "'. Error is: " + e.getMessage());
			}
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
				this.insight.getChromeDriver().captureImage(feUrl, url, imagePath, sessionId, waitTime);
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
		
		// TODO: Should we make this a parameter we pass in via the map and search for?
		// TODO: Iterate over full tree and don't search on specific elements
		Elements allElements = doc.getAllElements();
		
		// Create array list of dimensions for each signature element
		// OLD: List<String> widthHeightStyle = new ArrayList<String>();
		List<Attributes> elementAttributes = new ArrayList<Attributes>();
		Attributes elementAttrs;
		
		//List<String> searchLabels = new ArrayList<String>();
		//searchLabels.add("pdfsearchterm");
		
		// Add style for each element to list
		for (Element element : allElements) {
			elementAttrs = element.attributes();
			//System.out.println("Attributes: " + elementAttrs);

			if (element.hasAttr("pdfobject")) {
				elementAttrs = element.attributes();
				elementAttributes.add(elementAttrs);
					
				// WIP - add element text to search labels automatically
//				if(element.text().isEmpty() || element.text() == null) {
//					element.text("pdfsearchterm");
//				} else {
//					searchLabels.add(element.text());
//					System.out.println("Element text: " + element.text());
//				}
				
			}
		}
		
		
		// Convert from html to xhtml
		doc.outputSettings().syntax(Document.OutputSettings.Syntax.xml);

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "pdf");
		// grab file path to write the file
		String outputFileLocation = this.keyValue.get(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (outputFileLocation == null || outputFileLocation.isEmpty()) {
			outputFileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			outputFileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			outputFileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(outputFileLocation);

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
			fos = new FileOutputStream(outputFileLocation);
			ITextRenderer renderer = new ITextRenderer();
	        renderer.setDocument(tempXhtml.getAbsoluteFile());
	        renderer.layout();
	        renderer.createPDF(fos);
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
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		boolean addSignatureBlock = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_SIGNATURE_BLOCK.getKey()) + "");
		boolean addPageNumbers = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_PAGE_NUMBERS.getKey()) + "");
		if (addSignatureBlock || addPageNumbers) {
			PDDocument document = null;
			try {
				logger.info("Creating signature field...");
				document = PDFUtility.createDocument(outputFileLocation);
				if(addSignatureBlock) {
//					String signatureLabel = this.keyValue.get(ReactorKeysEnum.PDF_SIGNATURE_LABEL.getKey());
//					if(signatureLabel != null && !(signatureLabel=signatureLabel.trim()).isEmpty()) {
//						PDFUtility.addSignatureLabel(document, signatureLabel);
//					}
					//PDFUtility.addSignatureBlock(document);
					
					List<RectanglePage> rectPageList = new ArrayList<RectanglePage>();
					
					// Get list of labels to apply a signature field to
					List<String> searchLabels = getLabels();
					
					// WIP - run when we've already gotten the labels from the elements automatically
//					try {
//						getLabels();
//						List<String> searchLabels2 = getLabels();
//						
//						for (String term : searchLabels2) {
//							if (!searchLabels.contains(term)) {
//								searchLabels.add(term);
//							}
//						}
//					} catch (Exception e) {
//					}

					List<pageLocation> pageLocationList = new ArrayList<pageLocation>();
					ArrayList<FormObject> formObjectList = new ArrayList<FormObject>();
					
					pageLocationList.addAll(PDFUtility.findWordLocation(document, searchLabels));
					
					
//					for (pageLocation pl : pageLocationList ) {
//						System.out.println("Page Location: " + pl.keyword);
//					}
//					
//					for (Attributes pl : elementAttributes ) {
//						System.out.println("Attributes: " + pl);
//					}
					
					formObjectList.addAll(PDFUtility.setFormObjectLocation(elementAttributes, pageLocationList));
					PDFUtility.addPDFObjects(document, formObjectList);
					
				}
				if(addPageNumbers) {
					boolean ignoreFirstPage = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_PAGE_NUMBERS_IGNORE_FIRST.getKey()) + "");
					int startingNumber = 1;
					String startPageInput = this.keyValue.get(ReactorKeysEnum.PDF_START_PAGE_NUM.getKey());
					if(startPageInput != null && !(startPageInput=startPageInput.trim()).isEmpty())
					try {
						startingNumber = Integer.parseInt(startPageInput);
					} catch(Exception ignore) {
						
					}
					PDFUtility.addPageNumbers(document, startingNumber, ignoreFirstPage);
				}
	            document.save(outputFileLocation);
				logger.info("Done creating signature field...");
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(document != null) {
					try {
						document.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
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
		
		System.out.println(outputFileLocation);
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the pdf file"));
		return retNoun;
	}
	
	
	private List<String> getLabels () {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PDF_SIGNATURE_LABEL.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<String> labels = grs.getAllStrValues();
			if (labels != null && !labels.isEmpty()) {
				return labels;
			}
			
		}
		
		return null;
	}
	
	private Map<String, Object> mustacheVariables() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if(grs != null && !grs.isEmpty()) {
			Object obj = grs.get(0);
			if(!(obj instanceof Map)) {
				throw new IllegalArgumentException(ReactorKeysEnum.MUSTACHE_VARMAP.getKey() + " must be a map object");
			}
			return (Map<String, Object>) obj;
		}
		
		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}
		
		return null;
	}
	
}
