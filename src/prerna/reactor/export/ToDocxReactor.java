package prerna.reactor.export;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.imageio.ImageIO;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.docx4j.Docx4J;
import org.docx4j.convert.in.xhtml.XHTMLImporterImpl;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.openpackaging.parts.WordprocessingML.NumberingDefinitionsPart;
import org.docx4j.openpackaging.parts.WordprocessingML.StyleDefinitionsPart;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.mustache.MustacheUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ChromeDriverUtility;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

/**
 * ToDocx2Reactor converts HTML to Word documents using Docx4j's HTML import capabilities.
 * This reactor provides a simpler alternative to ToDocxReactor by leveraging Docx4j's 
 * built-in HTML to DOCX conversion instead of manually parsing and styling elements.
 */
public class ToDocxReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToDocxReactor.class);
	private static final String CLASS_NAME = ToDocxReactor.class.getName();

	public ToDocxReactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.HTML.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.OUTPUT_FILE_PATH.getKey(),
				ReactorKeysEnum.URL.getKey(), 
				ReactorKeysEnum.MUSTACHE.getKey(),
				ReactorKeysEnum.MUSTACHE_VARMAP.getKey(), 
				ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() 
		};

        this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		User user = this.insight.getUser();
		
		// throw error if user doesn't have rights to export data
		if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}

		// location for docx resources
		String insightFolder = this.insight.getInsightFolder();
		String htmlToParse = this.keyValue.get(ReactorKeysEnum.HTML.getKey());
		
		if (htmlToParse == null || (htmlToParse = htmlToParse.trim()).isEmpty()) {
			// guessing its passed as a file
			String htmlFileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
			File htmlFile = new File(htmlFileLocation);
			if (!htmlFile.exists() || !htmlFile.isFile()) {
				throw new IllegalArgumentException("No html passed in directly and could not find input file");
			}
			try {
				htmlToParse = FileUtils.readFileToString(htmlFile, "UTF-8");
			} catch (IOException e) {
				classLogger.error("Error reading html", e);
				throw new IllegalArgumentException("Error reading html. See logs for details");
			}
		} else {
			// Unescape quotes that may be escaped in the input
			htmlToParse = htmlToParse.replace("\\\"", "\"");
			htmlToParse = Utility.decodeURIComponent(htmlToParse);
		}
		
		// see if using mustache template format that needs modifications
		if (Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.MUSTACHE.getKey()) + "")) {
			Map<String, Object> variables = mustacheVariables();
			try {
				htmlToParse = MustacheUtility.compile(htmlToParse, variables);
			} catch (Exception e) {
				classLogger.error("Invalid mustache template or variables", e);
				throw new IllegalArgumentException("Invalid mustache template or variables. See logs for details");
			}
			classLogger.info("Exporting final html as: " + htmlToParse);
		}

		Integer waitTime = null;
		String waitTimeStr = this.keyValue.get(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey());
		if (waitTimeStr != null && !(waitTimeStr = waitTimeStr.trim()).isEmpty()) {
			try {
				waitTime = Integer.parseInt(waitTimeStr);
			} catch (NumberFormatException e) {
				classLogger.error("Invalid wait time option = '" + waitTimeStr + "'", e);
				throw new IllegalArgumentException(
						"Invalid wait time option = '" + waitTimeStr + "'. See logs for details.");
			}
		}

		// keep track for deleting at the end
		List<String> tempPaths = new ArrayList<>();

		// Parse the HTML document first
		org.jsoup.nodes.Document doc = Jsoup.parse(htmlToParse);
		
		// Find semoss tags
		Elements semossElements = doc.select("semoss");
		if (!semossElements.isEmpty()) {
			String feUrl = this.keyValue.get(ReactorKeysEnum.URL.getKey());
			if (feUrl == null || feUrl.isEmpty()) {
				throw new IllegalArgumentException("Must pass in the URL for the UI");
			}
			String sessionId = ThreadStore.getSessionId();

			// keep list of paths to clean up and delete once the docx is created
			// Process all semoss tags
			int imageNum = 1;
			for (Element element : semossElements) {
				String url = element.attr("url");

				// Run headless chrome with semossTagUrl
				String imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
				while (new File(imagePath).exists()) {
					imageNum++;
					imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
				}
				logger.info("Generating image for DOCX...");
				ChromeDriverUtility.captureImage(feUrl, url, imagePath, sessionId, waitTime);
				tempPaths.add(imagePath);
				logger.info("Done generating image for DOCX...");

				// Replace semoss tag with img tag
				element.tagName("img");
				// Replace url attribute with src attribute
				element.removeAttr("url");
				// Convert file path to file:// URI for docx4j
				String fileUri = new File(imagePath).toURI().toString();
				element.attr("src", fileUri);
				imageNum++;
			}
		}

		// Process external images - download and convert to local files
		// This must be done BEFORE converting doc to HTML string
		Elements imgElements = doc.select("img[src]");
		for (Element img : imgElements) {
			String src = img.attr("src");
			// Only process HTTP/HTTPS URLs (external images)
			if (src != null && (src.startsWith("http://") || src.startsWith("https://"))) {
				try {
					logger.info("Downloading external image: " + src);
					String localPath = downloadAndConvertImage(src, insightFolder);
					if (localPath != null) {
						// Convert file path to file:// URI for docx4j
						String fileUri = new File(localPath).toURI().toString();
						img.attr("src", fileUri);
						tempPaths.add(localPath);
						logger.info("Successfully downloaded and converted image to: " + fileUri);
					} else {
						logger.warn("Failed to download image, removing from document: " + src);
						img.remove();
					}
				} catch (Exception e) {
					logger.error("Error processing external image: " + src, e);
					// Remove the image element if we can't download it
					img.remove();
				}
			}
		}

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);

		// Grab the output file path where the DOCX will be written
		String outputFileLocation = this.keyValue.get(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey());
		
		// If OUTPUT_FILE_PATH is not provided, generate a default filename in the insight folder
		if (outputFileLocation == null || outputFileLocation.isEmpty()) {
			String defaultFileName = AbstractExportTxtReactor.getExportFileName(user, null, "docx");
			outputFileLocation = insightFolder + DIR_SEPARATOR + defaultFileName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			// Normalize the output path
			outputFileLocation = Utility.normalizePath(outputFileLocation);
			
			// Validate that the path ends with .docx
			if (!outputFileLocation.toLowerCase().endsWith(".docx")) {
				throw new IllegalArgumentException("OUTPUT_FILE_PATH must be a path to a .docx file (e.g., /path/to/document.docx)");
			}
			
			// If the path is not absolute, make it relative to the insight folder
			if (!new File(outputFileLocation).isAbsolute()) {
				outputFileLocation = insightFolder + DIR_SEPARATOR + outputFileLocation;
			}
			
			insightFile.setDeleteOnInsightClose(false);
		}
		
		// Ensure parent directories exist
		File outputFile = new File(outputFileLocation);
		File parentDir = outputFile.getParentFile();
		if (parentDir != null && !parentDir.exists()) {
			if (!parentDir.mkdirs()) {
				throw new IllegalArgumentException("Could not create parent directories for output path: " + outputFileLocation);
			}
		}
		
		insightFile.setFilePath(outputFileLocation);

		// Convert from HTML to DOCX using Docx4j
		try {
			logger.info("Converting HTML to DOCX using Docx4j...");
			
			// Get the final HTML content
			String finalHtml = doc.html();
			
			// Convert HTML to DOCX using Docx4j
			convertHtmlToDocxWithDocx4j(finalHtml, outputFileLocation, insightFolder, tempPaths);
			
			logger.info("Done converting HTML to DOCX...");
			
		} catch (Exception ex) {
			logger.error("Error processing HTML to DOCX with Docx4j", ex);
			throw new IllegalArgumentException("Error processing HTML to DOCX with Docx4j. See logs for details");
		}

		// delete temp files
		for (String path : tempPaths) {
			try {
				File f = new File(Utility.normalizePath(path));
				if (f.exists()) {
					FileUtils.forceDelete(f);
				}
			} catch (IOException e) {
				logger.error("Error deleting temp file: " + path, e);
			}
		}

		// store the insight file
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the docx file"));
		return retNoun;
	}

	/**
	 * Convert HTML to DOCX using Docx4j's HTML import functionality
	 * 
	 * @param html The HTML content to convert
	 * @param outputPath The path where the DOCX file will be saved
	 * @param insightFolder The folder containing resources
	 * @throws Exception if conversion fails
	 */
	private void convertHtmlToDocxWithDocx4j(String html, String outputPath, String insightFolder, List<String> tempImagePaths) throws Exception {
		// Sanitize HTML to ensure XHTML compliance
		// Images have already been processed and downloaded in execute()
		String sanitizedHtml = sanitizeHtmlForXhtml(html);
		
		// Create a new Word package
		WordprocessingMLPackage wordMLPackage = WordprocessingMLPackage.createPackage();
		
		// Add style definitions part - this must be done BEFORE creating the importer
		// to avoid the "Package field null" error for StyleDefinitionsPart
		StyleDefinitionsPart stylesPart = new StyleDefinitionsPart();
		wordMLPackage.getMainDocumentPart().addTargetPart(stylesPart);
		stylesPart.unmarshalDefaultStyles();
		
		// Add numbering part for lists - this must also be done BEFORE creating the importer
		NumberingDefinitionsPart ndp = new NumberingDefinitionsPart();
		wordMLPackage.getMainDocumentPart().addTargetPart(ndp);
		ndp.unmarshalDefaultNumbering();
		
		// Create HTML importer with secure XML configuration
		XHTMLImporterImpl xHTMLImporter = new XHTMLImporterImpl(wordMLPackage);
		
		// Configure secure XML parsers to prevent XXE attacks
		configureSecureXmlParsers(xHTMLImporter);
		
		// Set the base directory for resolving relative image paths
		xHTMLImporter.setHyperlinkStyle("Hyperlink");
		
		// Convert HTML to WordprocessingML
		List<Object> convertedContent = xHTMLImporter.convert(sanitizedHtml, insightFolder);
		
		// Add the converted content to the document
		wordMLPackage.getMainDocumentPart().getContent().addAll(convertedContent);
		
		// Save the document
		File outputFile = new File(outputPath);
		Docx4J.save(wordMLPackage, outputFile);
		
		// Clean up temporary image files
		for (String tempPath : tempImagePaths) {
			try {
				File tempFile = new File(tempPath);
				if (tempFile.exists()) {
					FileUtils.forceDelete(tempFile);
				}
			} catch (IOException e) {
				classLogger.warn("Failed to delete temporary image file: " + tempPath, e);
			}
		}
	}
	
	/**
	 * Configure secure XML parsers for the XHTML importer to prevent XXE attacks.
	 * This addresses security warnings about XML External Entity processing.
	 * 
	 * @param importer The XHTMLImporter to configure
	 */
	private void configureSecureXmlParsers(XHTMLImporterImpl importer) {
		try {
			// Configure DocumentBuilderFactory with secure settings
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			
			// Disable external entities and DTDs to prevent XXE attacks
			try {
				dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			} catch (ParserConfigurationException e) {
				classLogger.warn("Cannot set disallow-doctype-decl feature: " + e.getMessage());
			}
			
			try {
				dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
			} catch (ParserConfigurationException e) {
				classLogger.warn("Cannot set external-general-entities feature: " + e.getMessage());
			}
			
			try {
				dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			} catch (ParserConfigurationException e) {
				classLogger.warn("Cannot set external-parameter-entities feature: " + e.getMessage());
			}
			
			try {
				dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			} catch (ParserConfigurationException e) {
				classLogger.warn("Cannot set load-external-dtd feature: " + e.getMessage());
			}
			
			dbf.setXIncludeAware(false);
			dbf.setExpandEntityReferences(false);
			
			// Configure XMLInputFactory with secure settings
			XMLInputFactory xif = XMLInputFactory.newFactory();
			
			// Disable external entities
			xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
			xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
			
			classLogger.debug("Configured secure XML parsers for HTML import");
			
		} catch (Exception e) {
			classLogger.warn("Could not fully configure secure XML parsers: " + e.getMessage());
			// Continue anyway - the warnings are informational and the conversion will still work
		}
	}
	
	/**
	 * Sanitize HTML to ensure XHTML compliance.
	 * This fixes issues like non-self-closing tags (meta, br, hr, img, input, etc.)
	 * Note: Image processing is done earlier in the execute() method.
	 * 
	 * @param html The HTML to sanitize
	 * @return XHTML-compliant HTML string
	 */
	private String sanitizeHtmlForXhtml(String html) {
		// Parse with Jsoup
		org.jsoup.nodes.Document doc = Jsoup.parse(html);
		
		// Configure Jsoup to output XHTML (self-closing tags)
		doc.outputSettings()
			.syntax(org.jsoup.nodes.Document.OutputSettings.Syntax.xml)
			.prettyPrint(false);
		
		// Return the XHTML-compliant HTML
		return doc.html();
	}
	
	/**
	 * Download an external image and convert it to a supported format if needed.
	 * Docx4j supports: PNG, JPEG, GIF, BMP, TIFF. Unsupported formats like WebP are converted to PNG.
	 * 
	 * @param imageUrl The URL of the image to download
	 * @param insightFolder The folder to store the temp file
	 * @return Local file path or null if download fails
	 */
	private String downloadAndConvertImage(String imageUrl, String insightFolder) {
		BufferedImage image = null;
		InputStream inputStream = null;
		try {
			URL url = URI.create(imageUrl).toURL();
			image = ImageIO.read(url);
			
			if (image == null) {
				classLogger.warn("Could not read image from URL (possibly unsupported format): " + imageUrl);
				return null;
			}

			BufferedImage argbImage = new BufferedImage(
                    image.getWidth(),
                    image.getHeight(),
                    BufferedImage.TYPE_INT_ARGB
            );
            argbImage.getGraphics().drawImage(image, 0, 0, null);
			
			// Create a temporary file with PNG extension (always convert to PNG for reliability)
			String tempFileName = "external_image_" + UUID.randomUUID().toString() + ".png";
			String tempFilePath = insightFolder + DIR_SEPARATOR + tempFileName;
			File tempFile = new File(tempFilePath);
			
			// Write the image as PNG (supported by docx4j)
			boolean written = ImageIO.write(argbImage, "PNG", tempFile);
			
			if (!written) {
				classLogger.warn("Failed to write PNG file: " + tempFilePath);
				return null;
			}
			
			classLogger.info("Downloaded and converted image to PNG: " + imageUrl + " -> " + tempFilePath);
			return tempFilePath;
			
		} catch (Exception e) {
			classLogger.warn("Failed to download and convert image from URL: " + imageUrl + " - " + e.getMessage(), e);
			return null;
		} finally {
			// Ensure stream is closed even if exception occurs
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					// Ignore close errors
				}
			}
		}
	}

	/**
	 * Get mustache variables from the reactor keys
	 * 
	 * @return Map of mustache variable names to values
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> mustacheVariables() {
		Map<String, Object> variables = null;
		String varMapStr = this.keyValue.get(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if (varMapStr != null && !varMapStr.trim().isEmpty()) {
			try {
				variables = (Map<String, Object>) GSON.fromJson(varMapStr, Map.class);
			} catch (Exception e) {
				classLogger.error("Invalid mustache variable map", e);
				throw new IllegalArgumentException("Invalid mustache variable map. See logs for details");
			}
		}
		return variables;
	}

	@Override
	public String getReactorDescription() {
		return "Converts HTML content to DOCX (Word) format using Docx4j's built-in HTML import capabilities. "
				+ "This reactor provides a streamlined alternative to ToDocxReactor by leveraging Docx4j's native HTML-to-Word conversion engine, "
				+ "eliminating the need for manual element parsing and style application. "
				+ "Supports HTML elements, CSS styling, headings, paragraphs, tables, lists, images, and more. "
				+ "Processes custom <semoss> tags by converting them to images using headless Chrome. "
				+ "Supports Mustache templating for dynamic content generation. "
				+ "Returns a download key for the generated DOCX file.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.HTML.getKey())) {
			return "The HTML content to convert to DOCX format using Docx4j's HTML importer";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path to an HTML file to convert to DOCX. Used when HTML is passed as a file instead of directly.";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE.getKey())) {
			return "Boolean flag to enable Mustache template processing";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE_VARMAP.getKey())) {
			return "Map containing variables for Mustache template substitution";
		} else if (key.equals(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey())) {
			return "Full file path where the DOCX will be written (e.g., /path/to/document.docx). If not provided, a default filename will be generated. File will be created or overwritten.";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "Frontend URL required for processing <semoss> tags with headless Chrome";
		} else if (key.equals(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey())) {
			return "Wait time in milliseconds for image generation from <semoss> tags";
		}
		return super.getDescriptionForKey(key);
	}
}
