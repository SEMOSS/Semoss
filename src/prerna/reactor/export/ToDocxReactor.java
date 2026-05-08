/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.export;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
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
 * Converts HTML to Word documents using Docx4j's HTML import capabilities.
 */
public class ToDocxReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToDocxReactor.class);

	private static final String CLASS_NAME = ToDocxReactor.class.getName();

	public ToDocxReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.MARKDOWN.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.OUTPUT_FILE_PATH.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.MUSTACHE.getKey(), ReactorKeysEnum.MUSTACHE_VARMAP.getKey(),
				ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	/**
	 * Executes the HTML to DOCX conversion process. Reads HTML content from input
	 * or file, processes Mustache templates if enabled, converts semoss tags to
	 * images via headless Chrome, downloads external images, and generates the
	 * final DOCX file using Docx4j.
	 * 
	 * @return NounMetadata containing the download key for the generated DOCX file
	 */
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		User user = this.insight.getUser();

		if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}

		String insightFolder = this.insight.getInsightFolder();
		String htmlToParse = this.keyValue.get(ReactorKeysEnum.HTML.getKey());
		String markdownToParse = this.keyValue.get(ReactorKeysEnum.MARKDOWN.getKey());

		boolean hasHtml = htmlToParse != null && !htmlToParse.trim().isEmpty();
		boolean hasMarkdown = markdownToParse != null && !markdownToParse.trim().isEmpty();

		if (hasHtml && hasMarkdown) {
			throw new IllegalArgumentException("Only one of 'html' or 'markdown' may be provided, not both");
		}

		if (hasMarkdown) {
			logger.info("Converting markdown to HTML");
			htmlToParse = convertMarkdownToHtml(markdownToParse);
		} else if (!hasHtml) {
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
			htmlToParse = htmlToParse.replace("\\\"", "\"");
		}

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

		List<String> tempPaths = new ArrayList<>();
		org.jsoup.nodes.Document doc = Jsoup.parse(htmlToParse);

		Elements semossElements = doc.select("semoss");
		if (!semossElements.isEmpty()) {
			String feUrl = this.keyValue.get(ReactorKeysEnum.URL.getKey());
			if (feUrl == null || feUrl.isEmpty()) {
				throw new IllegalArgumentException("Must pass in the URL for the UI");
			}
			String sessionId = ThreadStore.getSessionId();
			int imageNum = 1;
			for (Element element : semossElements) {
				String url = element.attr("url");
				String imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
				while (new File(imagePath).exists()) {
					imageNum++;
					imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
				}
				logger.info("Generating image for DOCX...");
				ChromeDriverUtility.captureImage(feUrl, url, imagePath, sessionId, waitTime);
				tempPaths.add(imagePath);
				logger.info("Done generating image for DOCX...");

				element.tagName("img");
				element.removeAttr("url");
				String fileUri = new File(imagePath).toURI().toString();
				element.attr("src", fileUri);
				imageNum++;
			}
		}

		Elements imgElements = doc.select("img[src]");
		for (Element img : imgElements) {
			String src = img.attr("src");
			if (src != null && (src.startsWith("http://") || src.startsWith("https://"))) {
				try {
					logger.info("Downloading external image: " + src);
					String localPath = downloadAndConvertImage(src, insightFolder);
					if (localPath != null) {
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
					img.remove();
				}
			}
		}

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);

		// Get file name and location
		String prefixName = Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "docx");
		String outputFileLocation = this.keyValue.get(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey());

		if (outputFileLocation == null || outputFileLocation.isEmpty()) {
			outputFileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		} else {
			outputFileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(outputFileLocation);

		File outputFile = new File(outputFileLocation);
		File parentDir = outputFile.getParentFile();
		if (parentDir != null && !parentDir.exists()) {
			if (!parentDir.mkdirs()) {
				throw new IllegalArgumentException(
						"Could not create parent directories for output path: " + outputFileLocation);
			}
		}

		insightFile.setFilePath(outputFileLocation);

		try {
			logger.info("Converting HTML to DOCX using Docx4j...");
			String finalHtml = doc.html();
			convertHtmlToDocxWithDocx4j(finalHtml, outputFileLocation, insightFolder, tempPaths);

			logger.info("Done converting HTML to DOCX...");

		} catch (Exception ex) {
			logger.error("Error processing HTML to DOCX with Docx4j", ex);
			throw new IllegalArgumentException("Error processing HTML to DOCX with Docx4j. See logs for details");
		}

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

		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the docx file"));
		return retNoun;
	}

	/**
	 * Converts markdown content to an HTML string.
	 *
	 * @param markdown The markdown content to convert
	 * @return A full HTML document string
	 */
	private String convertMarkdownToHtml(String markdown) {
		List<Extension> extensions = Arrays.asList(TablesExtension.create());
		Parser parser = Parser.builder().extensions(extensions).build();
		HtmlRenderer renderer = HtmlRenderer.builder().extensions(extensions).build();
		Node document = parser.parse(markdown);
		String body = renderer.render(document);
		return "<!DOCTYPE html><html><head><meta charset=\"UTF-8\">"
				+ "<style>table { border-collapse: collapse; width: 100%; } "
				+ "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; } "
				+ "th { background-color: #f2f2f2; font-weight: bold; }</style>" + "</head><body>" + body
				+ "</body></html>";
	}

	/**
	 * Converts HTML to DOCX format using Docx4j library. Creates a Word package,
	 * configures styles and numbering, imports HTML content, and saves the result
	 * to the specified output path.
	 * 
	 * @param html           The HTML content to convert
	 * @param outputPath     The file path where the DOCX will be saved
	 * @param insightFolder  The base folder for resolving relative image paths
	 * @param tempImagePaths List of temporary image files to clean up after
	 *                       conversion
	 * @throws Exception if conversion or file operations fail
	 */
	private void convertHtmlToDocxWithDocx4j(String html, String outputPath, String insightFolder,
			List<String> tempImagePaths) throws Exception {
		String sanitizedHtml = sanitizeHtmlForXhtml(html);

		WordprocessingMLPackage wordMLPackage = WordprocessingMLPackage.createPackage();

		StyleDefinitionsPart stylesPart = new StyleDefinitionsPart();
		wordMLPackage.getMainDocumentPart().addTargetPart(stylesPart);
		stylesPart.unmarshalDefaultStyles();

		NumberingDefinitionsPart ndp = new NumberingDefinitionsPart();
		wordMLPackage.getMainDocumentPart().addTargetPart(ndp);
		ndp.unmarshalDefaultNumbering();

		XHTMLImporterImpl xHTMLImporter = new XHTMLImporterImpl(wordMLPackage);
		configureSecureXmlParsers(xHTMLImporter);
		xHTMLImporter.setHyperlinkStyle("Hyperlink");

		List<Object> convertedContent = xHTMLImporter.convert(sanitizedHtml, insightFolder);
		wordMLPackage.getMainDocumentPart().getContent().addAll(convertedContent);

		File outputFile = new File(outputPath);
		Docx4J.save(wordMLPackage, outputFile);

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
	 * Configures secure XML parsers to prevent XXE (XML External Entity) attacks.
	 * Disables external entity processing and DTD loading for
	 * DocumentBuilderFactory and XMLInputFactory to enhance security during
	 * HTML/XML parsing.
	 * 
	 * @param importer The XHTML importer to configure with secure settings
	 */
	private void configureSecureXmlParsers(XHTMLImporterImpl importer) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

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

			XMLInputFactory xif = XMLInputFactory.newFactory();
			xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
			xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
		} catch (Exception e) {
			classLogger.warn("Could not fully configure secure XML parsers: " + e.getMessage());
		}
	}

	/**
	 * Sanitizes HTML to ensure XHTML compliance. Converts HTML to XHTML format with
	 * properly self-closing tags (br, img, meta, etc.) required by Docx4j's XML
	 * parser.
	 * 
	 * @param html The HTML content to sanitize
	 * @return XHTML-compliant HTML string
	 */
	private String sanitizeHtmlForXhtml(String html) {
		org.jsoup.nodes.Document doc = Jsoup.parse(html);
		doc.outputSettings().syntax(org.jsoup.nodes.Document.OutputSettings.Syntax.xml).prettyPrint(false);
		return doc.html();
	}

	/**
	 * Downloads an external image from a URL and converts it to PNG format. All
	 * images are converted to PNG to ensure compatibility with Docx4j. Handles
	 * format conversion for unsupported formats like WebP.
	 * 
	 * @param imageUrl      The URL of the image to download
	 * @param insightFolder The folder where the temporary image file will be stored
	 * @return The local file path of the downloaded and converted image, or null if
	 *         download fails
	 */
	private String downloadAndConvertImage(String imageUrl, String insightFolder) {
		BufferedImage image;
		try {
			URL url = URI.create(imageUrl).toURL();
			image = ImageIO.read(url);

			if (image == null) {
				classLogger.warn("Could not read image from URL (possibly unsupported format): " + imageUrl);
				return null;
			}

			String tempFileName = "external_image_" + UUID.randomUUID().toString() + ".png";
			String tempFilePath = insightFolder + DIR_SEPARATOR + tempFileName;
			File tempFile = new File(tempFilePath);

			boolean written = ImageIO.write(image, "PNG", tempFile);

			if (!written) {
				classLogger.warn("Failed to write PNG file: " + tempFilePath);
				return null;
			}

			classLogger.info("Downloaded and converted image to PNG: " + imageUrl + " -> " + tempFilePath);
			return tempFilePath;

		} catch (Exception e) {
			classLogger.warn("Failed to download and convert image from URL: " + imageUrl + " - " + e.getMessage(), e);
			return null;
		}
	}

	/**
	 * Retrieves Mustache template variables from reactor input. Parses the
	 * JSON-encoded variable map provided via MUSTACHE_VARMAP key.
	 * 
	 * @return Map of variable names to values for Mustache template substitution,
	 *         or null if not provided
	 * @throws IllegalArgumentException if the variable map JSON is invalid
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> mustacheVariables() {
		Map<String, Object> variables = null;
		String varMapStr = this.keyValue.get(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if (varMapStr != null && !varMapStr.trim().isEmpty()) {
			try {
				variables = GSON.fromJson(varMapStr, Map.class);
			} catch (Exception e) {
				classLogger.error("Invalid mustache variable map", e);
				throw new IllegalArgumentException("Invalid mustache variable map. See logs for details");
			}
		}
		return variables;
	}

	@Override
	public String getReactorDescription() {
		return "Converts HTML to DOCX using Docx4j. Supports HTML elements, CSS, tables, lists, images. "
				+ "Processes <semoss> tags via headless Chrome. Supports Mustache templating.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.HTML.getKey())) {
			return "HTML content to convert to DOCX - only use if there is no markdown input";
		} else if (key.equals(ReactorKeysEnum.MARKDOWN.getKey())) {
			return "Markdown content to convert to DOCX - only use if there is no html input";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path to HTML file to convert";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE.getKey())) {
			return "Enable Mustache template processing";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE_VARMAP.getKey())) {
			return "Variables for Mustache template substitution";
		} else if (key.equals(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey())) {
			return "Output file path (e.g., /path/to/document.docx)";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "Frontend URL for processing <semoss> tags";
		} else if (key.equals(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey())) {
			return "Wait time in milliseconds for <semoss> image generation";
		}
		return super.getDescriptionForKey(key);
	}
}
