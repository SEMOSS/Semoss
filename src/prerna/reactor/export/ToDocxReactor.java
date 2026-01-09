package prerna.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.Units;
import org.apache.poi.xwpf.usermodel.BreakType;
import org.apache.poi.xwpf.usermodel.LineSpacingRule;
import org.apache.poi.xwpf.usermodel.ParagraphAlignment;
import org.apache.poi.xwpf.usermodel.UnderlinePatterns;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.CTShd;
import org.openxmlformats.schemas.wordprocessingml.x2006.main.STShd;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.InsightFile;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.mustache.MustacheUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class ToDocxReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToDocxReactor.class);
	private static final String CLASS_NAME = ToDocxReactor.class.getName();
	
	// Track parent block element styles for inheritance
	private Map<String, String> parentBlockStyles = new HashMap<>();

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
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);
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
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);
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
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);
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
				this.insight.getChromeDriver().captureImage(feUrl, url, imagePath, sessionId, waitTime);
				tempPaths.add(imagePath);
				logger.info("Done generating image for DOCX...");

				// Replace semoss tag with img tag
				element.tagName("img");
				// Replace url attribute with src attribute
				element.removeAttr("url");
				element.attr("src", imagePath);
				imageNum++;
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

		// Convert from html to docx
		try (XWPFDocument document = new XWPFDocument();
			 FileOutputStream out = new FileOutputStream(outputFileLocation)) {
			
			logger.info("Converting html to DOCX...");
			convertHtmlToDocx(doc.body(), document, insightFolder);
			document.write(out);
			logger.info("Done converting html to DOCX...");
			
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error creating DOCX file. See logs for details");
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
			throw new IllegalArgumentException("Error processing HTML to DOCX. See logs for details");
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

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the docx file"));
		return retNoun;
	}

	/**
	 * Convert HTML elements to DOCX format
	 * @param element The HTML element to convert
	 * @param document The DOCX document
	 * @param insightFolder The folder containing resources
	 */
	private void convertHtmlToDocx(Element element, XWPFDocument document, String insightFolder) {
		for (Node node : element.childNodes()) {
			if (node instanceof TextNode) {
				TextNode textNode = (TextNode) node;
				String text = textNode.text().trim();
				if (!text.isEmpty()) {
					XWPFParagraph paragraph = document.createParagraph();
					XWPFRun run = paragraph.createRun();
					run.setText(text);
				}
			} else if (node instanceof Element) {
				Element childElement = (Element) node;
				processElement(childElement, document, insightFolder);
			}
		}
	}

	/**
	 * Process individual HTML elements and convert to appropriate DOCX elements
	 * @param element The HTML element to process
	 * @param document The DOCX document
	 * @param insightFolder The folder containing resources
	 */
	private void processElement(Element element, XWPFDocument document, String insightFolder) {
		String tagName = element.tagName().toLowerCase();
		
		switch (tagName) {
			case "h1":
			case "h2":
			case "h3":
			case "h4":
			case "h5":
			case "h6":
				createHeading(element, document, tagName);
				break;
			case "p":
				createParagraph(element, document, insightFolder);
				break;
			case "table":
				createTable(element, document, insightFolder);
				break;
			case "ul":
			case "ol":
				createList(element, document, insightFolder, tagName.equals("ol"));
				break;
			case "img":
				createImage(element, document, insightFolder);
				break;
			case "br":
				XWPFParagraph brPara = document.createParagraph();
				brPara.createRun().addBreak();
				break;
			case "hr":
				XWPFParagraph hrPara = document.createParagraph();
				hrPara.createRun().addBreak(BreakType.TEXT_WRAPPING);
				break;
			case "div":
			case "section":
			case "article":
				// Parse block element styles to apply to children
				Map<String, String> blockStyles = parseInlineStyles(element);
				
				// Save current parent styles and merge with new ones (new styles override)
				Map<String, String> previousStyles = new HashMap<>(parentBlockStyles);
				Map<String, String> mergedStyles = new HashMap<>(parentBlockStyles);
				mergedStyles.putAll(blockStyles); // Inner styles override outer styles
				parentBlockStyles = mergedStyles;
				
				// Process children of block elements
				for (Node child : element.childNodes()) {
					if (child instanceof Element) {
						processElement((Element) child, document, insightFolder);
					} else if (child instanceof TextNode) {
						String text = ((TextNode) child).text().trim();
						if (!text.isEmpty()) {
							XWPFParagraph paragraph = document.createParagraph();
							XWPFRun run = paragraph.createRun();
							run.setText(text);
							// Apply merged parent block styles to the paragraph
							applyStylesToParagraph(paragraph, mergedStyles);
							// Apply merged parent block text formatting to the run
							applyStylesToRun(run, mergedStyles, false, 11);
						}
					}
				}
				
				// Restore previous parent styles
				parentBlockStyles = previousStyles;
				
				// Check for border-bottom style and insert horizontal line (only if this div has it directly)
				if (blockStyles.containsKey("border-bottom") && !blockStyles.get("border-bottom").isEmpty()) {
					classLogger.info("Found border-bottom on " + tagName + " element: " + blockStyles.get("border-bottom"));
					createHorizontalLine(document, blockStyles.get("border-bottom"));
				}
				break;
			default:
				// For inline elements or unknown elements, process as paragraph
				if (!element.text().trim().isEmpty()) {
					createParagraph(element, document, insightFolder);
				}
				break;
		}
	}

	/**
	 * Create a heading in the DOCX document
	 */
	private void createHeading(Element element, XWPFDocument document, String tagName) {
		XWPFParagraph paragraph = document.createParagraph();
		XWPFRun run = paragraph.createRun();
		run.setText(element.text());
		
		// Parse inline styles if present
		Map<String, String> styles = parseInlineStyles(element);
		
		// Set default font size based on heading level
		int fontSize = 24; // h1
		switch (tagName) {
			case "h2": fontSize = 20; break;
			case "h3": fontSize = 18; break;
			case "h4": fontSize = 16; break;
			case "h5": fontSize = 14; break;
			case "h6": fontSize = 12; break;
		}
		
		// Apply styles - inline styles override defaults
		applyStylesToRun(run, styles, true, fontSize);
		applyStylesToParagraph(paragraph, styles);
		
		// Apply parent block styles (e.g., text-align from parent div)
		if (!parentBlockStyles.isEmpty()) {
			applyStylesToParagraph(paragraph, parentBlockStyles);
		}
	}

	/**
	 * Create a paragraph in the DOCX document with proper formatting
	 */
	private void createParagraph(Element element, XWPFDocument document, String insightFolder) {
		XWPFParagraph paragraph = document.createParagraph();
		
		// Apply paragraph-level styles
		Map<String, String> styles = parseInlineStyles(element);
		applyStylesToParagraph(paragraph, styles);
		
		// Apply parent block styles (e.g., text-align from parent div)
		if (!parentBlockStyles.isEmpty()) {
			applyStylesToParagraph(paragraph, parentBlockStyles);
		}
		
		// Process children to handle inline formatting
		processInlineElements(element, paragraph, document, insightFolder);
	}

	/**
	 * Process inline elements like bold, italic, links, etc.
	 */
	private void processInlineElements(Element element, XWPFParagraph paragraph, XWPFDocument document, String insightFolder) {
		for (Node node : element.childNodes()) {
			if (node instanceof TextNode) {
				String text = ((TextNode) node).text();
				if (!text.isEmpty()) {
					XWPFRun run = paragraph.createRun();
					run.setText(text);
					// Apply parent styles and formatting
					Map<String, String> parentStyles = parseInlineStyles(element);
					applyStylesToRun(run, parentStyles, false, 11);
					applyParentFormatting(element, run);
				}
			} else if (node instanceof Element) {
				Element childElement = (Element) node;
				String tagName = childElement.tagName().toLowerCase();
				
				if (tagName.equals("img")) {
					createImage(childElement, document, insightFolder);
				} else if (tagName.equals("br")) {
					paragraph.createRun().addBreak();
				} else {
					XWPFRun run = paragraph.createRun();
					run.setText(childElement.text());
					
					// Parse inline styles
					Map<String, String> childStyles = parseInlineStyles(childElement);
					
					// Apply default formatting based on tag
					boolean defaultBold = false;
					boolean defaultItalic = false;
					String defaultColor = null;
					String defaultFont = null;
					boolean defaultUnderline = false;
					
					switch (tagName) {
						case "b":
						case "strong":
							defaultBold = true;
							break;
						case "i":
						case "em":
							defaultItalic = true;
							break;
						case "u":
							defaultUnderline = true;
							break;
						case "a":
							defaultUnderline = true;
							defaultColor = "0000FF";
							break;
						case "code":
							defaultFont = "Courier New";
							break;
					}
					
					// Apply styles (inline styles override defaults)
					applyStylesToRun(run, childStyles, defaultBold, 11, defaultItalic, defaultUnderline, defaultColor, defaultFont);
					
					// Apply parent formatting
					applyParentFormatting(element, run);
				}
			}
		}
	}

	/**
	 * Apply formatting from parent elements
	 */
	private void applyParentFormatting(Element element, XWPFRun run) {
		Element current = element;
		while (current != null) {
			String tagName = current.tagName().toLowerCase();
			switch (tagName) {
				case "b":
				case "strong":
					run.setBold(true);
					break;
				case "i":
				case "em":
					run.setItalic(true);
					break;
				case "u":
					run.setUnderline(UnderlinePatterns.SINGLE);
					break;
			}
			current = current.parent();
		}
	}

	/**
	 * Create a table in the DOCX document
	 */
	private void createTable(Element tableElement, XWPFDocument document, String insightFolder) {
		Elements rows = tableElement.select("tr");
		if (rows.isEmpty()) {
			return;
		}
		
		// Create table
		XWPFTable table = document.createTable();
		
		boolean firstRow = true;
		int rowIndex = 0;
		for (Element row : rows) {
			XWPFTableRow tableRow;
			if (firstRow && rowIndex == 0) {
				tableRow = table.getRow(0);
				firstRow = false;
			} else {
				tableRow = table.createRow();
			}
			
			Elements cells = row.select("th, td");
			int cellIndex = 0;
			for (Element cell : cells) {
				XWPFTableCell tableCell;
				if (cellIndex < tableRow.getTableCells().size()) {
					tableCell = tableRow.getCell(cellIndex);
				} else {
					tableCell = tableRow.addNewTableCell();
				}
				
				// Set cell text
				XWPFParagraph cellParagraph = tableCell.getParagraphs().get(0);
				XWPFRun cellRun = cellParagraph.createRun();
				cellRun.setText(cell.text());
				
				// Apply styles to table cell
				Map<String, String> cellStyles = parseInlineStyles(cell);
				boolean isHeader = cell.tagName().equals("th");
				applyStylesToRun(cellRun, cellStyles, isHeader, 11);
				applyStylesToParagraph(cellParagraph, cellStyles);
				applyStylesToTableCell(tableCell, cellStyles);
				
				cellIndex++;
			}
			rowIndex++;
		}
	}

	/**
	 * Create a horizontal line in the DOCX document based on border style
	 * @param document The DOCX document
	 * @param borderValue CSS border value (e.g., "1px solid gray")
	 */
	private void createHorizontalLine(XWPFDocument document, String borderValue) {
		XWPFParagraph hrPara = document.createParagraph();
		hrPara.setSpacingBefore(0);
		hrPara.setSpacingAfter(0);
		
		// Apply the border style to the paragraph's bottom border
		parseBorderStyle(hrPara, borderValue);
	}

	/**
	 * Create a list in the DOCX document
	 */
	private void createList(Element listElement, XWPFDocument document, String insightFolder, boolean ordered) {
		Elements items = listElement.select("> li");
		int index = 1;
		for (Element item : items) {
			XWPFParagraph paragraph = document.createParagraph();
			XWPFRun run = paragraph.createRun();
			
			String prefix = ordered ? (index + ". ") : "• ";
			run.setText(prefix + item.text());
			
			// Apply styles from list item
			Map<String, String> itemStyles = parseInlineStyles(item);
			applyStylesToRun(run, itemStyles, false, 11);
			applyStylesToParagraph(paragraph, itemStyles);
			
			index++;
		}
	}

	/**
	 * Create an image in the DOCX document
	 */
	private void createImage(Element imgElement, XWPFDocument document, String insightFolder) {
		String src = imgElement.attr("src");
		if (src == null || src.isEmpty()) {
			return;
		}
		
		File imageFile = null;
		boolean isTemporary = false;
		
		// Check if src is a URL
		if (src.startsWith("http://") || src.startsWith("https://")) {
			try {
				// Download the image to a temporary file
				URL imageUrl = new URL(src);
				String fileName = UUID.randomUUID().toString();
				
				// Determine extension from URL or default to .png
				String extension = ".png";
				String urlPath = imageUrl.getPath();
				if (urlPath.contains(".")) {
					String urlExt = urlPath.substring(urlPath.lastIndexOf("."));
					// Validate extension
					if (urlExt.matches("\\.(png|jpg|jpeg|gif|bmp)")) {
						extension = urlExt;
					}
				}
				
				Path tempFile = Files.createTempFile(fileName, extension);
				try (InputStream in = imageUrl.openStream()) {
					Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
				}
				imageFile = tempFile.toFile();
				isTemporary = true;
				classLogger.info("Downloaded image from URL: " + src + " to " + imageFile.getAbsolutePath());
			} catch (Exception e) {
				classLogger.error("Error downloading image from URL: " + src, e);
				classLogger.error(Constants.STACKTRACE, e);
				return;
			}
		} else {
			// Handle local file paths
			imageFile = new File(src);
			if (!imageFile.isAbsolute()) {
				imageFile = new File(insightFolder, src);
			}
			
			if (!imageFile.exists()) {
				classLogger.warn("Image file not found: " + imageFile.getAbsolutePath());
				return;
			}
		}
		
		try (FileInputStream fis = new FileInputStream(imageFile)) {
			XWPFParagraph paragraph = document.createParagraph();
            paragraph.setAlignment(ParagraphAlignment.CENTER);
			XWPFRun run = paragraph.createRun();
			
			// Determine image format
			int format = XWPFDocument.PICTURE_TYPE_PNG;
			String fileName = imageFile.getName().toLowerCase();
			if (fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")) {
				format = XWPFDocument.PICTURE_TYPE_JPEG;
			} else if (fileName.endsWith(".gif")) {
				format = XWPFDocument.PICTURE_TYPE_GIF;
			} else if (fileName.endsWith(".bmp")) {
				format = XWPFDocument.PICTURE_TYPE_BMP;
			}
			
			// Add image with default width (6 inches = 6*914400 EMUs)
			run.addPicture(fis, format, imageFile.getName(), Units.toEMU(400), Units.toEMU(300));
			
		} catch (Exception e) {
			classLogger.error("Error adding image to DOCX: " + imageFile.getAbsolutePath(), e);
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			// Clean up temporary file if it was downloaded
			if (isTemporary && imageFile != null && imageFile.exists()) {
				try {
					Files.delete(imageFile.toPath());
					classLogger.debug("Deleted temporary image file: " + imageFile.getAbsolutePath());
				} catch (IOException e) {
					classLogger.warn("Failed to delete temporary image file: " + imageFile.getAbsolutePath(), e);
				}
			}
		}
	}

	/**
	 * Parse inline CSS styles from an HTML element's style attribute
	 * @param element The HTML element
	 * @return Map of CSS property names to values
	 */
	private Map<String, String> parseInlineStyles(Element element) {
		Map<String, String> styles = new HashMap<>();
		String styleAttr = element.attr("style");
		
		if (styleAttr != null && !styleAttr.isEmpty()) {
			// Parse style attribute: "property: value; property2: value2"
			String[] stylePairs = styleAttr.split(";");
			for (String pair : stylePairs) {
				String[] keyValue = pair.split(":", 2);
				if (keyValue.length == 2) {
					String property = keyValue[0].trim().toLowerCase();
					String value = keyValue[1].trim();
					styles.put(property, value);
				}
			}
		}
		
		return styles;
	}
	
	/**
	 * Apply CSS styles to an XWPFRun
	 * @param run The run to apply styles to
	 * @param styles Map of CSS styles
	 * @param defaultBold Default bold setting if no style override
	 * @param defaultFontSize Default font size if no style override
	 */
	private void applyStylesToRun(XWPFRun run, Map<String, String> styles, boolean defaultBold, int defaultFontSize) {
		applyStylesToRun(run, styles, defaultBold, defaultFontSize, false, false, null, null);
	}
	
	/**
	 * Apply CSS styles to an XWPFRun with additional defaults
	 * @param run The run to apply styles to
	 * @param styles Map of CSS styles
	 * @param defaultBold Default bold setting
	 * @param defaultFontSize Default font size
	 * @param defaultItalic Default italic setting
	 * @param defaultUnderline Default underline setting
	 * @param defaultColor Default color
	 * @param defaultFont Default font family
	 */
	private void applyStylesToRun(XWPFRun run, Map<String, String> styles, boolean defaultBold, 
			int defaultFontSize, boolean defaultItalic, boolean defaultUnderline, 
			String defaultColor, String defaultFont) {
		
		// Font weight (bold)
		if (styles.containsKey("font-weight")) {
			String weight = styles.get("font-weight");
			run.setBold(weight.equals("bold") || weight.equals("bolder") || 
						Integer.parseInt(weight.replaceAll("\\D", "")) >= 600);
		} else if (defaultBold) {
			run.setBold(true);
		}
		
		// Font style (italic)
		if (styles.containsKey("font-style")) {
			run.setItalic(styles.get("font-style").equals("italic") || 
						styles.get("font-style").equals("oblique"));
		} else if (defaultItalic) {
			run.setItalic(true);
		}
		
		// Text decoration (underline)
		if (styles.containsKey("text-decoration")) {
			String decoration = styles.get("text-decoration");
			if (decoration.contains("underline")) {
				run.setUnderline(UnderlinePatterns.SINGLE);
			}
		} else if (defaultUnderline) {
			run.setUnderline(UnderlinePatterns.SINGLE);
		}
		
		// Font size
		if (styles.containsKey("font-size")) {
			String fontSize = styles.get("font-size");
			int size = parseFontSize(fontSize, defaultFontSize);
			run.setFontSize(size);
		} else {
			run.setFontSize(defaultFontSize);
		}
		
		// Font family
		if (styles.containsKey("font-family")) {
			String fontFamily = styles.get("font-family");
			// Remove quotes and get first font
			fontFamily = fontFamily.replaceAll("['\"]|", "").split(",")[0].trim();
			run.setFontFamily(fontFamily);
		} else if (defaultFont != null) {
			run.setFontFamily(defaultFont);
		}
		
		// Color
		if (styles.containsKey("color")) {
			String color = parseColor(styles.get("color"));
			if (color != null) {
				run.setColor(color);
			}
		} else if (defaultColor != null) {
			run.setColor(defaultColor);
		}
		
		// Background color (highlight)
		if (styles.containsKey("background-color")) {
			String bgColor = parseColor(styles.get("background-color"));
			if (bgColor != null) {
				try {
					run.getCTR().addNewRPr().addNewHighlight().setVal(
							org.openxmlformats.schemas.wordprocessingml.x2006.main.STHighlightColor.Enum.forString(bgColor));
				} catch (Exception e) {
					// If highlight color not supported, ignore
					classLogger.debug("Background color not applied: " + bgColor);
				}
			}
		}
	}
	
	/**
	 * Apply CSS styles to an XWPFParagraph
	 * @param paragraph The paragraph to apply styles to
	 * @param styles Map of CSS styles
	 */
	private void applyStylesToParagraph(XWPFParagraph paragraph, Map<String, String> styles) {
		// Text alignment
		if (styles.containsKey("text-align")) {
			String align = styles.get("text-align").toLowerCase();
			classLogger.debug("Applying text-align: " + align);
			switch (align) {
				case "left":
					paragraph.setAlignment(ParagraphAlignment.LEFT);
					break;
				case "center":
					paragraph.setAlignment(ParagraphAlignment.CENTER);
					break;
				case "right":
					paragraph.setAlignment(ParagraphAlignment.RIGHT);
					break;
				case "justify":
					paragraph.setAlignment(ParagraphAlignment.BOTH);
					break;
			}
		} else {
			classLogger.debug("No text-align found in styles: " + styles.keySet());
		}
		
		// Line spacing
		if (styles.containsKey("line-height")) {
			try {
				String lineHeight = styles.get("line-height");
				if (lineHeight.endsWith("%")) {
					int spacing = Integer.parseInt(lineHeight.replace("%", "").trim());
					paragraph.setSpacingLineRule(LineSpacingRule.AUTO);
					paragraph.setSpacingBetween(spacing / 100.0);
				}
			} catch (Exception e) {
				classLogger.debug("Line height not applied: " + styles.get("line-height"));
			}
		}
		
		// Borders
		applyBordersToParagraph(paragraph, styles);
	}
	
	/**
	 * Apply CSS border styles to an XWPFParagraph
	 * @param paragraph The paragraph to apply borders to
	 * @param styles Map of CSS styles
	 */
	private void applyBordersToParagraph(XWPFParagraph paragraph, Map<String, String> styles) {
		try {
			// Only handle border-bottom for horizontal lines
			if (styles.containsKey("border-bottom")) {
				parseBorderStyle(paragraph, styles.get("border-bottom"));
			}
		} catch (Exception e) {
			classLogger.debug("Border-bottom style not applied", e);
		}
	}
	
	/**
	 * Parse a CSS border declaration and apply to paragraph bottom border
	 * @param paragraph The paragraph
	 * @param borderValue CSS border value (e.g., "1px solid gray")
	 */
	private void parseBorderStyle(XWPFParagraph paragraph, String borderValue) {
		if (borderValue == null || borderValue.isEmpty()) {
			return;
		}
		
		// Parse: "1px solid gray" or "2pt dashed #000000"
		String[] parts = borderValue.trim().split("\\s+");
		if (parts.length < 2) {
			return;
		}
		
		// Default values
		int size = 4; // Default size in eighths of a point
		org.openxmlformats.schemas.wordprocessingml.x2006.main.STBorder.Enum borderType = 
			org.openxmlformats.schemas.wordprocessingml.x2006.main.STBorder.SINGLE;
		String color = "000000"; // Default black
		
		// Parse width (e.g., "1px", "2pt")
		if (parts[0].matches("\\d+(\\.\\d+)?(px|pt|em)?")) {
			String widthStr = parts[0].replaceAll("[^\\d.]", "");
			try {
				double width = Double.parseDouble(widthStr);
				// Convert to eighths of a point (DOCX uses 1/8 pt units)
				if (parts[0].contains("px")) {
					width = width * 0.75; // 1px = 0.75pt at 96 DPI
				}
				size = (int) Math.max(2, width * 8); // Minimum 2, which is 1/4 pt
			} catch (NumberFormatException e) {
				classLogger.debug("Could not parse border width: " + parts[0]);
			}
		}
		
		// Parse style (solid, dashed, dotted, etc.)
		if (parts.length > 1) {
			String style = parts[1].toLowerCase();
			switch (style) {
				case "solid":
					borderType = org.openxmlformats.schemas.wordprocessingml.x2006.main.STBorder.SINGLE;
					break;
				case "dashed":
					borderType = org.openxmlformats.schemas.wordprocessingml.x2006.main.STBorder.DASHED;
					break;
				case "dotted":
					borderType = org.openxmlformats.schemas.wordprocessingml.x2006.main.STBorder.DOTTED;
					break;
				case "double":
					borderType = org.openxmlformats.schemas.wordprocessingml.x2006.main.STBorder.DOUBLE;
					break;
			}
		}
		
		// Parse color (if specified)
		if (parts.length > 2) {
			String parsedColor = parseColor(parts[2]);
			if (parsedColor != null) {
				color = parsedColor;
			}
		}
		
		// Apply the border to the bottom
		org.openxmlformats.schemas.wordprocessingml.x2006.main.CTPPr pPr = 
			paragraph.getCTP().getPPr() != null ? paragraph.getCTP().getPPr() : paragraph.getCTP().addNewPPr();
		
		org.openxmlformats.schemas.wordprocessingml.x2006.main.CTPBdr borders = 
			pPr.getPBdr() != null ? pPr.getPBdr() : pPr.addNewPBdr();
		
		org.openxmlformats.schemas.wordprocessingml.x2006.main.CTBorder border = 
			borders.isSetBottom() ? borders.getBottom() : borders.addNewBottom();
		
		border.setVal(borderType);
		border.setSz(java.math.BigInteger.valueOf(size));
		border.setColor(color);
		border.setSpace(java.math.BigInteger.valueOf(1));
	}
	
	/**
	 * Apply CSS styles to an XWPFTableCell
	 * @param cell The table cell to apply styles to
	 * @param styles Map of CSS styles
	 */
	private void applyStylesToTableCell(XWPFTableCell cell, Map<String, String> styles) {
		// Background color
		if (styles.containsKey("background-color")) {
			String bgColor = parseColor(styles.get("background-color"));
			if (bgColor != null) {
				try {
					CTShd shd = cell.getCTTc().addNewTcPr().addNewShd();
					shd.setFill(bgColor);
					shd.setVal(STShd.CLEAR);
				} catch (Exception e) {
					classLogger.debug("Cell background color not applied: " + bgColor, e);
				}
			}
		}
	}
	
	/**
	 * Parse CSS color value to hex format (without #)
	 * @param color CSS color value
	 * @return Hex color without # prefix, or null if cannot parse
	 */
	private String parseColor(String color) {
		if (color == null || color.isEmpty()) {
			return null;
		}
		
		color = color.trim().toLowerCase();
		
		// Hex color
		if (color.startsWith("#")) {
			return color.substring(1);
		}
		
		// RGB/RGBA color
		if (color.startsWith("rgb")) {
			Pattern pattern = Pattern.compile("rgba?\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,\\s*(\\d+)");
			Matcher matcher = pattern.matcher(color);
			if (matcher.find()) {
				int r = Integer.parseInt(matcher.group(1));
				int g = Integer.parseInt(matcher.group(2));
				int b = Integer.parseInt(matcher.group(3));
				return String.format("%02X%02X%02X", r, g, b);
			}
		}
		
		// Named colors - common web colors
		Map<String, String> namedColors = new HashMap<>();
		namedColors.put("black", "000000");
		namedColors.put("white", "FFFFFF");
		namedColors.put("red", "FF0000");
		namedColors.put("green", "008000");
		namedColors.put("blue", "0000FF");
		namedColors.put("yellow", "FFFF00");
		namedColors.put("gray", "808080");
		namedColors.put("grey", "808080");
		namedColors.put("orange", "FFA500");
		namedColors.put("purple", "800080");
		namedColors.put("pink", "FFC0CB");
		namedColors.put("brown", "A52A2A");
		
		return namedColors.get(color);
	}
	
	/**
	 * Parse CSS font size to points
	 * @param fontSize CSS font size value
	 * @param defaultSize Default size to use if cannot parse
	 * @return Font size in points
	 */
	private int parseFontSize(String fontSize, int defaultSize) {
		if (fontSize == null || fontSize.isEmpty()) {
			return defaultSize;
		}
		
		fontSize = fontSize.trim().toLowerCase();
		
		try {
			// Points (pt)
			if (fontSize.endsWith("pt")) {
				return Integer.parseInt(fontSize.replace("pt", "").trim());
			}
			// Pixels (px) - approximate conversion (96 DPI)
			else if (fontSize.endsWith("px")) {
				int px = Integer.parseInt(fontSize.replace("px", "").trim());
				return (int) Math.round(px * 0.75); // 1pt = 1.333px at 96 DPI
			}
			// Em - relative to default
			else if (fontSize.endsWith("em")) {
				double em = Double.parseDouble(fontSize.replace("em", "").trim());
				return (int) Math.round(defaultSize * em);
			}
			// Percentage
			else if (fontSize.endsWith("%")) {
				int percent = Integer.parseInt(fontSize.replace("%", "").trim());
				return (int) Math.round(defaultSize * percent / 100.0);
			}
			// Named sizes
			else {
				switch (fontSize) {
					case "xx-small": return 7;
					case "x-small": return 8;
					case "small": return 10;
					case "medium": return 12;
					case "large": return 14;
					case "x-large": return 18;
					case "xx-large": return 24;
					default: return defaultSize;
				}
			}
		} catch (NumberFormatException e) {
			classLogger.debug("Could not parse font size: " + fontSize);
			return defaultSize;
		}
	}
	
	/**
	 * Get mustache variables from the reactor store
	 */
	private Map<String, Object> mustacheVariables() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if (grs != null && !grs.isEmpty()) {
			Object obj = grs.get(0);
			if (!(obj instanceof Map)) {
				throw new IllegalArgumentException(ReactorKeysEnum.MUSTACHE_VARMAP.getKey() + " must be a map object");
			}
			return (Map<String, Object>) obj;
		}

		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		if (mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}

		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Converts HTML content to DOCX (Word) format with comprehensive formatting support. "
				+ "Supports HTML elements including headings, paragraphs, tables, lists, images, and styling. "
				+ "Processes custom <semoss> tags by converting them to images using headless Chrome. "
				+ "Supports Mustache templating for dynamic content generation. "
				+ "Returns a download key for the generated DOCX file.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.HTML.getKey())) {
			return "The HTML content to convert to DOCX format";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Path to an HTML file to convert to DOCX. Used when HTML is passed as a file instead of directly.";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE.getKey())) {
			return "Boolean flag to enable Mustache template processing";
		} else if (key.equals(ReactorKeysEnum.MUSTACHE_VARMAP.getKey())) {
			return "Map containing variables for Mustache template substitution";
		} else if (key.equals(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey())) {
			return "Required: Full file path where the DOCX will be written (e.g., /path/to/document.docx). File will be created or overwritten.";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "Frontend URL required for processing <semoss> tags with headless Chrome";
		} else if (key.equals(ReactorKeysEnum.IMAGE_WAIT_TIME.getKey())) {
			return "Wait time in milliseconds for image generation from <semoss> tags";
		}
		return super.getDescriptionForKey(key);
	}
}
