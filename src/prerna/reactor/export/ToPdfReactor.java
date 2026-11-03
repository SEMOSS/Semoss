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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xhtmlrenderer.pdf.ITextRenderer;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.mustache.MustacheUtility;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.reactor.export.pdf.PDFUtility.FormObject;
import prerna.reactor.export.pdf.PDFUtility.pageLocation;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

/**
 * ToPdfReactor - Converts HTML content to PDF format
 * 
 * This reactor provides two independent PDF generation approaches: 1. Modern
 * Playwright-based generation (default) - Better CSS support and performance 2.
 * Legacy IText renderer - Backwards compatibility with existing features
 * 
 * Features: - Mustache template processing for dynamic content - Custom
 * <semoss> tag processing with screenshot generation - PDF post-processing
 * (signatures, page numbers) - Parallel image generation for improved
 * performance
 * 
 * Code Organization: - Utility methods for common operations - HTML content
 * processing section - PDF generation approaches section - Playwright PDF
 * generation methods - Legacy IText PDF generation methods - Common helper
 * methods - Reactor description methods
 */
public class ToPdfReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToPdfReactor.class);
	private static final String CLASS_NAME = ToPdfReactor.class.getName();

	@SuppressWarnings("deprecation")
	public ToPdfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HTML.getKey(), ReactorKeysEnum.MARKDOWN.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.OUTPUT_FILE_PATH.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.MUSTACHE.getKey(),
				ReactorKeysEnum.MUSTACHE_VARMAP.getKey(), ReactorKeysEnum.PDF_SIGNATURE_BLOCK.getKey(),
				ReactorKeysEnum.PDF_SIGNATURE_LABEL.getKey(), ReactorKeysEnum.PDF_PAGE_NUMBERS.getKey(),
				ReactorKeysEnum.PDF_PAGE_NUMBERS_IGNORE_FIRST.getKey(), ReactorKeysEnum.PDF_START_PAGE_NUM.getKey(),
				ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		User user = this.insight.getUser();

		// Validate user export permissions
		if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}

		// Determine PDF generation approach
		boolean usePlaywright = false;

		String boolString = Utility.getDIHelperProperty(Constants.PLAYWRIGHT_EXPORT);
		if (boolString != null) {
			usePlaywright = Boolean.parseBoolean(boolString);
		}

		// Initialize common variables
		String insightFolder = this.insight.getInsightFolder();
		List<Path> tempPaths = new ArrayList<>();
		Integer waitTime = getWaitTime();

		// Get and process HTML content
		String htmlToParse;
		try {
			htmlToParse = getHtmlContent(classLogger, tempPaths);

			// Process Mustache templates if needed
			if (getBooleanValue(ReactorKeysEnum.MUSTACHE.getKey(), false)) {
				htmlToParse = processMustacheTemplate(htmlToParse);
			}
		} catch (IOException e) {
			logger.error("Failed to process input HTML content before PDF generation", e);
			throw new IllegalArgumentException("Failed to process HTML content: " + e.getMessage(), e);
		}

		// Execute appropriate PDF generation approach
		if (usePlaywright) {
			return executePlaywrightApproach(htmlToParse, insightFolder, tempPaths, waitTime, user, logger);
		} else {
			return executeLegacyApproach(htmlToParse, insightFolder, tempPaths, waitTime, user, logger);
		}
	}

	// ==================== UTILITY METHODS ====================

	/**
	 * Calculate an appropriate font size for table cells based on the
	 * maximum number of columns in any table row within the provided HTML.
	 * More columns → smaller font so the table fits within the page width.
	 */
	private int calculateTableFontSizePx(String htmlContent) {
		org.jsoup.nodes.Document doc = Jsoup.parse(htmlContent);
		int maxCols = 0;
		for (org.jsoup.nodes.Element row : doc.select("tr")) {
			int cols = row.select("th, td").size();
			if (cols > maxCols) {
				maxCols = cols;
			}
		}
		if (maxCols <= 5)
			return 12;
		if (maxCols <= 8)
			return 10;
		if (maxCols <= 12)
			return 9;
		if (maxCols <= 16)
			return 8;
		return 7;
	}

	/**
	 * Get boolean value from keyValue map with default fallback
	 */
	private boolean getBooleanValue(String key, boolean defaultValue) {
		String value = this.keyValue.get(key);
		return (value != null && !value.trim().isEmpty()) ? Boolean.parseBoolean(value.trim()) : defaultValue;
	}

	private List<String> getLabels() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.PDF_SIGNATURE_LABEL.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<String> labels = grs.getAllStrValues();
			if (labels != null && !labels.isEmpty()) {
				return labels;
			}

		}
		return null;
	}

	// ==================== HTML CONTENT PROCESSING ====================

	/**
	 * Get HTML content from direct input, file, or markdown
	 */
	private String getHtmlContent(Logger logger, List<Path> tempPaths) throws IOException {
		String htmlContent = this.keyValue.get(ReactorKeysEnum.HTML.getKey());
		String markdownContent = this.keyValue.get(ReactorKeysEnum.MARKDOWN.getKey());
		String htmlFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());

		boolean hasHtml = htmlContent != null && !htmlContent.trim().isEmpty();
		boolean hasMarkdown = markdownContent != null && !markdownContent.trim().isEmpty();

		if (hasHtml && hasMarkdown) {
			throw new IllegalArgumentException("Only one of 'html' or 'markdown' may be provided, not both");
		}

		if (hasMarkdown) {
			logger.info("Converting markdown to HTML");
			return convertMarkdownToHtml(markdownContent);
		}

		if (htmlFilePath != null && !htmlFilePath.trim().isEmpty()) {
			String htmlFileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
			Path htmlFile = Paths.get(htmlFileLocation);

			if (!Files.exists(htmlFile) || !Files.isRegularFile(htmlFile)) {
				throw new IllegalArgumentException("HTML file not found: " + htmlFileLocation);
			}

			htmlContent = Files.readString(htmlFile, StandardCharsets.UTF_8);
			logger.info("Loaded HTML from file: {}", htmlFileLocation);
		}
		return htmlContent;
	}

	/**
	 * Convert markdown content to an HTML string
	 */
	private String convertMarkdownToHtml(String markdown) {
		List<Extension> extensions = Arrays.asList(TablesExtension.create());
		Parser parser = Parser.builder().extensions(extensions).build();
		HtmlRenderer renderer = HtmlRenderer.builder().extensions(extensions).build();
		Node document = parser.parse(markdown);
		String body = renderer.render(document);
		return "<!DOCTYPE html><html><head><meta charset=\"UTF-8\">"
				+ "<style>table { border-collapse: collapse; width: 100%; } "
				+ "th, td { border: 1px solid #ddd; padding: 4px; text-align: left; } "
				+ "th { background-color: #f2f2f2; font-weight: bold; }</style>" + "</head><body>" + body
				+ "</body></html>";
	}

	/**
	 * Process Mustache templates
	 */
	private String processMustacheTemplate(String htmlContent) {
		Map<String, Object> variables = mustacheVariables();
		try {
			String compiled = MustacheUtility.compile(htmlContent, variables);
			classLogger.debug("Mustache template compiled successfully");
			return compiled;
		} catch (Exception e) {
			classLogger.error("Failed to compile mustache template for PDF export", e);
			throw new IllegalArgumentException("Invalid mustache template or variables", e);
		}
	}

	/**
	 * Get wait time for image generation
	 */
	private Integer getWaitTime() {
		String waitTimeStr = this.keyValue.get(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey());
		if (waitTimeStr != null && !waitTimeStr.trim().isEmpty()) {
			try {
				return Integer.parseInt(waitTimeStr.trim());
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid wait time: {}, using default", waitTimeStr);
			}
		}
		return 10000; // Default 10 seconds
	}

	private Map<String, Object> mustacheVariables() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if (grs != null && !grs.isEmpty()) {
			Object obj = grs.get(0);
			if (!(obj instanceof Map)) {
				throw new IllegalArgumentException(ReactorKeysEnum.MUSTACHE_VARMAP.getKey() + " must be a map object");
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) obj;
			return result;
		}

		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		if (mapInput != null && !mapInput.isEmpty()) {
			@SuppressWarnings("unchecked")
			Map<String, Object> result = (Map<String, Object>) mapInput.get(0);
			return result;
		}

		return null;
	}

	/**
	 * Legacy way to process custom <semoss> tags and convert to images
	 */
	private org.jsoup.nodes.Document processSemossTagsLegacy(String htmlToParse, String insightFolder,
			List<Path> tempPaths, Integer waitTime) {
		// Find semoss tags
		org.jsoup.nodes.Document doc = Jsoup.parse(htmlToParse);
		Elements semossElements = doc.select("semoss");
		if (!semossElements.isEmpty()) {
			String feUrl = this.keyValue.get(ReactorKeysEnum.URL.getKey());
			if (feUrl == null || feUrl.isEmpty()) {
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
				while (new File(imagePath).exists()) {
					imageNum++;
					imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
				}
				classLogger.info("Generating image for PDF...");
				this.insight.getChromeDriver().captureImage(feUrl, url, imagePath, sessionId, waitTime);
				tempPaths.add(Paths.get(imagePath));
				classLogger.info("Done generating image for PDF...");

				// Replace semoss tag with img tag
				element.tagName("img");
				// Replace url attribute with src attribute
				element.removeAttr("url");
				element.attr("src", "image" + imageNum + ".png");
				imageNum++;
			}
		}
		return doc;
	}

	/**
	 * Process custom <semoss> tags and convert to images (modern approach)
	 */
	private String processSemossTags(String htmlContent, String insightFolder, List<Path> tempPaths, Integer waitTime) {
		Document doc = Jsoup.parse(htmlContent);
		Elements semossElements = doc.select("semoss");

		if (semossElements.isEmpty()) {
			return htmlContent;
		}

		String feUrl = this.keyValue.get(ReactorKeysEnum.URL.getKey());
		if (feUrl == null || feUrl.isEmpty()) {
			throw new IllegalArgumentException("URL required for processing <semoss> tags");
		}

		// Process semoss tags in parallel for better performance
		List<CompletableFuture<Void>> futures = new ArrayList<>();
		int[] imageCounter = { 1 };

		for (Element element : semossElements) {
			String url = element.attr("url");
			int imageNum = imageCounter[0]++;

			CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
				try {
					String imagePath = generateImagePath(insightFolder, imageNum);
					captureScreenshot(feUrl + url, imagePath, waitTime);
					tempPaths.add(Paths.get(imagePath));

					// Replace semoss tag with img tag
					synchronized (element) {
						element.tagName("img");
						element.removeAttr("url");
						element.attr("src", "file:///" + imagePath);
						element.attr("style", "max-width: 100%; height: auto;");
					}
				} catch (Exception e) {
					classLogger.error("Failed to process semoss tag for URL '{}'", url, e);
				}
			});

			futures.add(future);
		}

		// Wait for all images to be generated
		CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

		return doc.html();
	}

	/**
	 * Add signature fields to PDF
	 */
	private void addSignatureFields(PDDocument document) throws Exception {
		List<String> searchLabels = getLabels();
		if (searchLabels == null || searchLabels.isEmpty()) {
			return;
		}

		// Use existing PDFUtility methods
		@SuppressWarnings("unused")
		List<pageLocation> pageLocations = PDFUtility.findWordLocation(document, searchLabels);

		// Convert to form objects and add to PDF
		// Implementation depends on your PDFUtility class
		// TODO: Implement signature field addition using pageLocations
	}

	/**
	 * Add signature fields to PDF (legacy approach)
	 */
	private void addLegacySignatureFields(PDDocument document, List<Attributes> elementAttributes) throws IOException {
		List<String> searchLabels = getLabels();
		if (searchLabels == null || searchLabels.isEmpty()) {
			return;
		}

		List<pageLocation> pageLocationList = new ArrayList<>();
		ArrayList<FormObject> formObjectList = new ArrayList<>();

		pageLocationList.addAll(PDFUtility.findWordLocation(document, searchLabels));
		formObjectList.addAll(PDFUtility.setFormObjectLocation(elementAttributes, pageLocationList));
		PDFUtility.addPDFObjects(document, formObjectList);
	}

	/**
	 * Add page numbers to PDF
	 */
	private void addPageNumbering(PDDocument document) {
		boolean ignoreFirstPage = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_PAGE_NUMBERS_IGNORE_FIRST.getKey()) + "");

		int startingNumber = 1;
		String startPageInput = this.keyValue.get(ReactorKeysEnum.PDF_START_PAGE_NUM.getKey());
		if (startPageInput != null && !startPageInput.trim().isEmpty()) {
			try {
				startingNumber = Integer.parseInt(startPageInput);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid starting page number, using default: 1");
			}
		}

		// Use existing PDFUtility method or implement page numbering
		try {
			int pageCount = document.getNumberOfPages();
			int currentPageNum = startingNumber;

			for (int i = 0; i < pageCount; i++) {
				if (ignoreFirstPage && i == 0) {
					continue;
				}

				PDPage page = document.getPage(i);
				PDPageContentStream contentStream = new PDPageContentStream(document, page,
						PDPageContentStream.AppendMode.APPEND, true, true);

				// Add page number at bottom center
				PDRectangle mediaBox = page.getMediaBox();
				float x = mediaBox.getWidth() / 2;
				float y = 20; // 20 points from bottom

				contentStream.beginText();
				contentStream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 10);
				contentStream.newLineAtOffset(x - 10, y); // Adjust for text width
				contentStream.showText(String.valueOf(currentPageNum));
				contentStream.endText();
				contentStream.close();

				currentPageNum++;
			}
		} catch (IOException e) {
			classLogger.error("Failed to add page numbers to PDF", e);
		}
	}

	/**
	 * Add page numbers to PDF (legacy approach)
	 */
	private void addLegacyPageNumbers(PDDocument document) throws IOException {
		boolean ignoreFirstPage = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_PAGE_NUMBERS_IGNORE_FIRST.getKey()) + "");

		int startingNumber = 1;
		String startPageInput = this.keyValue.get(ReactorKeysEnum.PDF_START_PAGE_NUM.getKey());
		if (startPageInput != null && !(startPageInput = startPageInput.trim()).isEmpty()) {
			try {
				startingNumber = Integer.parseInt(startPageInput);
			} catch (Exception ignore) {
				// Use default value
			}
		}

		PDFUtility.addPageNumbers(document, startingNumber, ignoreFirstPage);
	}

	/**
	 * Get output file path with proper naming
	 */
	@SuppressWarnings("deprecation")
	private String getOutputFilePath(String insightFolder) {
		String prefixName = Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(this.insight.getUser(), prefixName, "pdf");

		String outputFileLocation = this.keyValue.get(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey());
		if (outputFileLocation == null || outputFileLocation.isEmpty()) {
			return insightFolder + DIR_SEPARATOR + exportName;
		}

		return outputFileLocation + DIR_SEPARATOR + exportName;
	}

	/**
	 * Capture screenshot using Playwright
	 */
	private void captureScreenshot(String url, String outputPath, Integer waitTime) {
		try (Playwright pw = Playwright.create();
				Browser browser = pw.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
				Page page = browser.newPage()) {
			page.navigate(url);
			page.waitForLoadState();

			if (waitTime != null && waitTime > 0) {
				page.waitForTimeout(waitTime);
			}

			Page.ScreenshotOptions options = new Page.ScreenshotOptions().setPath(Paths.get(outputPath))
					.setFullPage(true);

			page.screenshot(options);
			classLogger.info("Screenshot captured: {}", outputPath);
		}
	}

	/**
	 * Generate unique image path
	 */
	private String generateImagePath(String insightFolder, int imageNum) {
		String imagePath = insightFolder + DIR_SEPARATOR + "image" + imageNum + ".png";
		int counter = imageNum;

		while (Files.exists(Paths.get(imagePath))) {
			counter++;
			imagePath = insightFolder + DIR_SEPARATOR + "image" + counter + ".png";
		}

		return imagePath;
	}

	// ==================== PDF GENERATION APPROACHES ====================

	/**
	 * Complete Playwright approach (toPdf2 style) - independent execution path
	 */
	private NounMetadata executePlaywrightApproach(String htmlToParse, String insightFolder, List<Path> tempPaths,
			Integer waitTime, User user, Logger logger) {

		try {
			// Process custom semoss tags
			htmlToParse = processSemossTags(htmlToParse, insightFolder, tempPaths, waitTime);

			// Generate PDF using Playwright
			String pdfPath = generatePdfWithPlaywright(htmlToParse, insightFolder);
			tempPaths.add(Paths.get(pdfPath));

			// Post-process PDF (signatures, page numbers)
			String finalPdfPath = postProcessPdf(pdfPath, insightFolder);

			// Prepare download response
			return prepareDownloadResponse(finalPdfPath, user);
		} catch (Exception e) {
			classLogger.error("Failed to generate PDF using Playwright workflow", e);
			throw new RuntimeException("Failed to generate PDF: " + e.getMessage(), e);
		} finally {
			// Cleanup temp files
			cleanupTempFiles(tempPaths);
		}
	}

	/**
	 * Generate PDF using Playwright's native PDF generation
	 */
	private String generatePdfWithPlaywright(String htmlContent, String insightFolder) throws IOException {
		// Scale table font-size based on column count so wide tables are not clipped at
		// the page boundary
		int fontSize = calculateTableFontSizePx(htmlContent);
		classLogger.info("PDF table font size calculated: {}px", fontSize);
		String tableStyleOverride = "<style>th, td { font-size: " + fontSize
				+ "px !important; padding: 4px !important; }</style>";
		if (htmlContent.contains("</head>")) {
			htmlContent = htmlContent.replace("</head>", tableStyleOverride + "</head>");
		} else {
			htmlContent = tableStyleOverride + htmlContent;
		}

		String tempHtmlPath = insightFolder + DIR_SEPARATOR + UUID.randomUUID() + ".html";
		Files.writeString(Paths.get(tempHtmlPath), htmlContent, StandardCharsets.UTF_8);

		String pdfPath = insightFolder + DIR_SEPARATOR + UUID.randomUUID() + ".pdf";

		try (Playwright pw = Playwright.create();
				Browser browser = pw.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
				Page page = browser.newPage();) {

			// Navigate to HTML file
			page.navigate("file:///" + tempHtmlPath);

			// Wait for page to be fully loaded
			page.waitForLoadState();

			// Generate PDF with options and standard margins
			Page.PdfOptions pdfOptions = new Page.PdfOptions().setPath(Paths.get(pdfPath)).setFormat("A4")
					.setPrintBackground(true).setPreferCSSPageSize(false);

			// Set standard 1 inch margins
			pdfOptions.setMargin(new com.microsoft.playwright.options.Margin().setTop("1in").setRight("1in")
					.setBottom("1in").setLeft("1in"));

			page.pdf(pdfOptions);
		}

		// Clean up temp HTML
		Files.deleteIfExists(Paths.get(tempHtmlPath));

		classLogger.info("PDF generated successfully: {}", pdfPath);
		return pdfPath;
	}

	/**
	 * Post-process PDF for signatures and page numbers
	 */
	private String postProcessPdf(String inputPdfPath, String insightFolder) throws Exception {
		boolean addSignatureBlock = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_SIGNATURE_BLOCK.getKey()) + "");
		boolean addPageNumbers = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_PAGE_NUMBERS.getKey()) + "");

		String outputPdfPath = getOutputFilePath(insightFolder);

		if (!addSignatureBlock && !addPageNumbers) {
			// Even if no post-processing, copy to proper filename
			Files.copy(Paths.get(inputPdfPath), Paths.get(outputPdfPath));
			classLogger.info("PDF copied to proper filename: {}", outputPdfPath);
			return outputPdfPath;
		}

		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(inputPdfPath))) {
			if (addSignatureBlock) {
				addSignatureFields(document);
			}

			if (addPageNumbers) {
				addPageNumbering(document);
			}
			document.save(outputPdfPath);
		}

		classLogger.info("PDF post-processing completed: {}", outputPdfPath);
		return outputPdfPath;
	}

	/**
	 * Prepare download response
	 */
	private NounMetadata prepareDownloadResponse(String pdfPath, User user) {
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(pdfPath);
		insightFile.setDeleteOnInsightClose(false);

		this.insight.addExportFile(downloadKey, insightFile);

		// Debug logging similar to original ToPdfReactor
		classLogger.info("Generated PDF at path: {}", pdfPath);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the PDF file"));

		return retNoun;
	}

	/**
	 * Complete Legacy approach (toPdf style) - independent execution path
	 */
	@SuppressWarnings("deprecation")
	private NounMetadata executeLegacyApproach(String htmlToParse, String insightFolder, List<Path> tempPaths,
			Integer waitTime, User user, Logger logger) {

		// Process semoss tags using legacy Chrome driver
		org.jsoup.nodes.Document doc = processSemossTagsLegacy(htmlToParse, insightFolder, tempPaths, waitTime);

		// Scale table font-size based on column count so wide tables are not clipped at
		// the page boundary
		int fontSize = calculateTableFontSizePx(htmlToParse);
		org.jsoup.nodes.Element docHead = doc.head();
		if (docHead != null) {
			docHead.appendElement("style")
					.text("th, td { font-size: " + fontSize + "px !important; padding: 4px !important; }");
		}

		Elements allElements = doc.getAllElements();

		// Create array list of dimensions for each signature element
		List<Attributes> elementAttributes = new ArrayList<Attributes>();

		// Add style for each element to list
		for (Element element : allElements) {
			if (element.hasAttr("pdfobject")) {
				Attributes elementAttrs = element.attributes();
				elementAttributes.add(elementAttrs);
			}
		}

		// Convert from html to xhtml
		doc.outputSettings().syntax(org.jsoup.nodes.Document.OutputSettings.Syntax.xml);

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);

		// Get file name and location
		String prefixName = Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "pdf");
		String outputFileLocation = this.keyValue.get(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey());

		if (outputFileLocation == null || outputFileLocation.isEmpty()) {
			outputFileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
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
			FileUtils.writeStringToFile(tempXhtml, doc.html(), Charset.forName("UTF-8"));
			tempPaths.add(Paths.get(tempXhtmlPath));
		} catch (IOException e1) {
			logger.error("Failed to write temporary XHTML file for PDF conversion", e1);
		}

		// Convert from xhtml to pdf using IText
		try (FileOutputStream fos = new FileOutputStream(outputFileLocation)) {
			DocumentBuilderFactory factory = Utility.getDocumentBuilderFactory();
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document document = builder.parse(tempXhtml);

			logger.info("Converting html to PDF...");
			ITextRenderer renderer = new ITextRenderer();
			renderer.setDocument(document);
			renderer.layout();
			renderer.createPDF(fos);
			logger.info("Done converting html to PDF...");
		} catch (Exception ex) {
			classLogger.error("Unable to convert from html to pdf due to error {}", ex.getMessage(), ex);
			throw new IllegalArgumentException("Error converting the database metamodel html file to pdf", ex);
		}

		boolean addSignatureBlock = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_SIGNATURE_BLOCK.getKey()) + "");
		boolean addPageNumbers = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.PDF_PAGE_NUMBERS.getKey()) + "");
		try (PDDocument document = PDFUtility.createDocument(outputFileLocation)) {
			if (addSignatureBlock || addPageNumbers) {
				try {
					if (addSignatureBlock) {
						logger.info("Creating signature field...");
						addLegacySignatureFields(document, elementAttributes);
						logger.info("Done creating signature field...");
					}

					if (addPageNumbers) {
						logger.info("Adding page numbers...");
						addLegacyPageNumbers(document);
						logger.info("Done adding page numbers...");
					}
					document.save(outputFileLocation);
					logger.info("PDF post-processing completed");
				} catch (IOException e) {
					logger.error("Failed during legacy PDF post-processing", e);
				} finally {
					if (document != null) {
						try {
							document.close();
						} catch (IOException e) {
							logger.error("Failed to close PDF document after post-processing", e);
						}
					}
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to create PDF document from generated file", e);
		}

		// Clean up temp files
		cleanupTempFiles(tempPaths);

		// Store the insight file
		this.insight.addExportFile(downloadKey, insightFile);

		// Debug logging for PDF generation
		logger.info("Legacy PDF generated at path: {}", outputFileLocation);
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the pdf file"));
		return retNoun;
	}

	/**
	 * Cleanup temporary files
	 */
	private void cleanupTempFiles(List<Path> tempPaths) {
		for (Path path : tempPaths) {
			try {
				Files.deleteIfExists(path);
				classLogger.debug("Deleted temp file: {}", path);
			} catch (IOException e) {
				classLogger.warn("Failed to delete temp file: {}", path, e);
			}
		}
	}

	// ==================== REACTOR DESCRIPTION METHODS ====================

	@Override
	public String getReactorDescription() {
		return "Converts HTML content to PDF format. Returns a download key for the generated PDF file.";
	}

	@Override
	@SuppressWarnings("deprecation")
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.HTML.getKey())) {
			return "The HTML content to convert to PDF format - Only use if there is no html file";
		} else if (key.equals(ReactorKeysEnum.MARKDOWN.getKey())) {
			return "The markdown content to convert to PDF format - Only use if there is no html input";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path to an HTML file to convert to PDF";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE.getKey())) {
			return "Boolean flag to enable Mustache template processing";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE_VARMAP.getKey())) {
			return "Map containing variables for Mustache template substitution";
		} else if (key.equals(ReactorKeysEnum.PDF_SIGNATURE_BLOCK.getKey())) {
			return "Boolean flag to add interactive signature fields to the PDF";
		} else if (key.equals(ReactorKeysEnum.PDF_PAGE_NUMBERS.getKey())) {
			return "Boolean flag to add page numbers to the PDF";
		} else if (key.equals(ReactorKeysEnum.PDF_PAGE_NUMBERS_IGNORE_FIRST.getKey())) {
			return "Boolean flag to skip page numbering on the first page";
		} else if (key.equals(ReactorKeysEnum.PDF_START_PAGE_NUM.getKey())) {
			return "Starting page number for PDF page numbering";
		} else if (key.equals(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey())) {
			return "Output directory path where the PDF file will be saved - should be empty by default";
		} else if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "Custom filename for the generated PDF (without extension)";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "Frontend URL required for processing <semoss> tags";
		} else if (key.equals(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey())) {
			return "Wait time in milliseconds for image generation from <semoss> tags";
		}
		return super.getDescriptionForKey(key);
	}
}
