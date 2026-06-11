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
package prerna.reactor.storage;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.converter.WordToHtmlConverter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.ParagraphAlignment;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetStorageFileAsBase64Reactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetStorageFileAsBase64Reactor.class);
	private static final String CONVERT_TO_PDF_KEY = "convertToPdf";

	// Supported conversion types
	private static final String EXT_DOCX = "docx";
	private static final String EXT_DOC = "doc";
	private static final String EXT_XLSX = "xlsx";
	private static final String EXT_PPTX = "pptx";
	private static final String EXT_PDF = "pdf";
	private static final String EXT_TXT = "txt";

	public GetStorageFileAsBase64Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				CONVERT_TO_PDF_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), storage.getEngineId())) {
			throw new IllegalArgumentException("User does not have permission to access this storage engine");
		}

		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		if (storagePath == null || storagePath.isEmpty()) {
			throw new IllegalArgumentException("Storage path is required");
		}

		String convertToPdfStr = this.keyValue.get(CONVERT_TO_PDF_KEY);
		boolean convertToPdf = "true".equalsIgnoreCase(convertToPdfStr);

		try {
			byte[] raw = storage.readBlobToMemory(storagePath);

			if (convertToPdf) {
				raw = maybeConvertToPdf(raw, storagePath);
			}

			String base64 = Base64.getEncoder().encodeToString(raw);
			return new NounMetadata(base64, PixelDataType.CONST_STRING);

		} catch (UnsupportedOperationException e) {
			classLogger.error("Failed to read storage file content as base64: {}", e.getMessage(), e);
			throw new IllegalArgumentException("In-memory blob reading is not supported by this storage engine");
		} catch (Exception e) {
			classLogger.error("Failed to read storage file content as base64: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Error reading blob from storage: " + storagePath);
		}
	}

	// ==================== CONVERSION ROUTING ====================

	/**
	 * Routes to the appropriate converter based on file extension. Returns original
	 * bytes unchanged for PDF, TXT, PPTX (skip), or unknown types.
	 */
	private byte[] maybeConvertToPdf(byte[] raw, String storagePath) throws Exception {
		String ext = getFileExtension(storagePath).toLowerCase();

		switch (ext) {
		case EXT_DOCX:
			classLogger.info("Converting DOCX to PDF: {}", storagePath);
			return htmlToPdfBytes(docxToHtml(raw));

		case EXT_DOC:
			classLogger.info("Converting DOC to PDF: {}", storagePath);
			return htmlToPdfBytes(docLegacyToHtml(raw));

		case EXT_XLSX:
			classLogger.info("Converting XLSX to PDF: {}", storagePath);
			return htmlToPdfBytes(xlsxToHtml(raw));

		case EXT_PPTX:
			classLogger.warn("PPTX conversion is not supported, returning raw bytes: {}", storagePath);
			return raw;

		case EXT_PDF:
		case EXT_TXT:
			classLogger.debug("No conversion needed for {}, passing through", ext);
			return raw;

		default:
			classLogger.warn("Unsupported extension '{}' for PDF conversion, returning raw bytes", ext);
			return raw;
		}
	}

	// ==================== FORMAT HTML CONVERTERS ====================

	/**
	 * Converts DOCX bytes to an HTML string using Apache POI XWPFDocument. Handles
	 * paragraphs (with heading styles), inline formatting (bold/italic/underline),
	 * and tables.
	 */
	private String docxToHtml(byte[] docxBytes) throws Exception {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(docxBytes); XWPFDocument doc = new XWPFDocument(bis)) {

			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html><head>").append("<meta charset='UTF-8'>").append("<style>")
					.append("body { font-family: Arial, sans-serif; margin: 40px; font-size: 12pt; }")
					.append("table { border-collapse: collapse; width: 100%; margin: 12px 0; }")
					.append("td, th { border: 1px solid #ccc; padding: 6px 10px; }")
					.append("th { background-color: #f0f0f0; font-weight: bold; }")
					.append("p { margin: 6px 0; line-height: 1.5; }")
					.append("h1,h2,h3,h4,h5,h6 { margin: 16px 0 8px 0; }").append("</style></head><body>");

			for (Object bodyElement : doc.getBodyElements()) {
				if (bodyElement instanceof XWPFParagraph) {
					html.append(paragraphToHtml((XWPFParagraph) bodyElement));
				} else if (bodyElement instanceof XWPFTable) {
					html.append(tableToHtml((XWPFTable) bodyElement));
				}
			}

			html.append("</body></html>");
			return html.toString();
		}
	}

	/**
	 * Converts a legacy binary .doc file to HTML using Apache POI
	 * WordToHtmlConverter. Requires poi-scratchpad.
	 */
	private String docLegacyToHtml(byte[] docBytes) throws Exception {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(docBytes); HWPFDocument doc = new HWPFDocument(bis)) {

			DocumentBuilderFactory factory = Utility.getDocumentBuilderFactory();
			org.w3c.dom.Document htmlDoc = factory.newDocumentBuilder().newDocument();

			WordToHtmlConverter converter = new WordToHtmlConverter(htmlDoc);
			converter.processDocument(doc);

			java.io.StringWriter sw = new java.io.StringWriter();
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			transformer.setOutputProperty(OutputKeys.METHOD, "html");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(new DOMSource(converter.getDocument()), new StreamResult(sw));
			return sw.toString();
		}
	}

	/**
	 * Converts XLSX bytes to an HTML string with one table per sheet. Uses
	 * DataFormatter so formulas and numbers render as their displayed values.
	 */
	private String xlsxToHtml(byte[] xlsxBytes) throws Exception {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(xlsxBytes);
				Workbook workbook = new XSSFWorkbook(bis)) {

			DataFormatter formatter = new DataFormatter();
			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html><head>").append("<meta charset='UTF-8'>").append("<style>")
					.append("body { font-family: Arial, sans-serif; margin: 40px; font-size: 11pt; }")
					.append("h2 { margin-top: 24px; color: #333; }")
					.append("table { border-collapse: collapse; margin: 12px 0; width: 100%; }")
					.append("td, th { border: 1px solid #ccc; padding: 5px 8px; white-space: nowrap; }")
					.append("tr:nth-child(even) { background-color: #f9f9f9; }")
					.append("th { background-color: #e0e0e0; font-weight: bold; }").append("</style></head><body>");

			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				Sheet sheet = workbook.getSheetAt(i);
				html.append("<h2>").append(escapeHtml(sheet.getSheetName())).append("</h2>");
				html.append("<table>");

				boolean firstRow = true;
				for (Row row : sheet) {
					if (isRowEmpty(row)) {
						continue;
					}

					String tag = firstRow ? "th" : "td";
					html.append("<tr>");
					for (int col = 0; col < row.getLastCellNum(); col++) {
						Cell cell = row.getCell(col, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
						String value = formatter.formatCellValue(cell);
						html.append("<").append(tag).append(">").append(escapeHtml(value)).append("</").append(tag)
								.append(">");
					}
					html.append("</tr>");
					firstRow = false;
				}

				html.append("</table>");
			}

			html.append("</body></html>");
			return html.toString();
		}
	}

	// ==================== HTML -> PDF VIA PLAYWRIGHT ====================

	/**
	 * Renders an HTML string to PDF bytes using Playwright's headless Chromium.
	 * Writes a temp HTML file, generates the PDF, reads it back, then cleans up.
	 */
	private byte[] htmlToPdfBytes(String html) throws Exception {
		String insightFolder = this.insight.getInsightFolder();
		String tempId = UUID.randomUUID().toString();
		Path tempHtml = Paths.get(insightFolder, tempId + ".html");
		Path tempPdf = Paths.get(insightFolder, tempId + ".pdf");

		try {
			Files.writeString(tempHtml, html, java.nio.charset.StandardCharsets.UTF_8);

			try (Playwright pw = Playwright.create();
					Browser browser = pw.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
					Page page = browser.newPage()) {
				page.navigate("file:///" + tempHtml.toAbsolutePath());
				page.waitForLoadState();

				page.pdf(new Page.PdfOptions().setPath(tempPdf).setFormat("A4").setPrintBackground(true)
						.setMargin(new com.microsoft.playwright.options.Margin().setTop("0.75in").setRight("0.75in")
								.setBottom("0.75in").setLeft("0.75in")));
			}

			return Files.readAllBytes(tempPdf);
		} finally {
			// Always clean up temp files
			Files.deleteIfExists(tempHtml);
			Files.deleteIfExists(tempPdf);
		}
	}

	// ==================== DOCX HTML HELPERS ====================

	private String paragraphToHtml(XWPFParagraph para) {
		String styleId = para.getStyleID();
		String text = runsToHtml(para.getRuns());

		if (text.trim().isEmpty()) {
			return "<p>&nbsp;</p>";
		}

		// Map Word heading styles to HTML heading tags
		if (styleId != null) {
			if (styleId.startsWith("Heading1") || styleId.equals("1")) {
				return "<h1>" + text + "</h1>";
			}
			if (styleId.startsWith("Heading2") || styleId.equals("2")) {
				return "<h2>" + text + "</h2>";
			}
			if (styleId.startsWith("Heading3") || styleId.equals("3")) {
				return "<h3>" + text + "</h3>";
			}
			if (styleId.startsWith("Heading4") || styleId.equals("4")) {
				return "<h4>" + text + "</h4>";
			}
		}

		// Alignment
		String align = "";
		ParagraphAlignment alignment = para.getAlignment();
		if (alignment == ParagraphAlignment.CENTER) {
			align = " style='text-align:center'";
		} else if (alignment == ParagraphAlignment.RIGHT) {
			align = " style='text-align:right'";
		} else if (alignment == ParagraphAlignment.BOTH) {
			align = " style='text-align:justify'";
		}

		return "<p" + align + ">" + text + "</p>";
	}

	private String runsToHtml(List<XWPFRun> runs) {
		StringBuilder sb = new StringBuilder();
		for (XWPFRun run : runs) {
			String text = escapeHtml(run.getText(0));
			if (text == null || text.isEmpty()) {
				continue;
			}

			if (run.isBold()) {
				text = "<strong>" + text + "</strong>";
			}
			if (run.isItalic()) {
				text = "<em>" + text + "</em>";
			}
			if (run.isStrikeThrough()) {
				text = "<s>" + text + "</s>";
			}
			if (run.getUnderline() != org.apache.poi.xwpf.usermodel.UnderlinePatterns.NONE) {
				text = "<u>" + text + "</u>";
			}

			sb.append(text);
		}
		return sb.toString();
	}

	private String tableToHtml(XWPFTable table) {
		StringBuilder sb = new StringBuilder("<table>");
		List<XWPFTableRow> rows = table.getRows();

		for (int r = 0; r < rows.size(); r++) {
			sb.append("<tr>");
			String tag = (r == 0) ? "th" : "td";
			for (XWPFTableCell cell : rows.get(r).getTableCells()) {
				String cellText = "";
				for (XWPFParagraph para : cell.getParagraphs()) {
					cellText += runsToHtml(para.getRuns());
				}
				sb.append("<").append(tag).append(">").append(cellText).append("</").append(tag).append(">");
			}
			sb.append("</tr>");
		}

		sb.append("</table>");
		return sb.toString();
	}

	// ==================== UTILITIES ====================

	private boolean isRowEmpty(Row row) {
		if (row == null) {
			return true;
		}
		for (Cell cell : row) {
			if (cell != null && cell.getCellType() != CellType.BLANK) {
				String val = new DataFormatter().formatCellValue(cell).trim();
				if (!val.isEmpty()) {
					return false;
				}
			}
		}
		return true;
	}

	private String escapeHtml(String text) {
		if (text == null) {
			return "";
		}
		return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
	}

	private String getFileExtension(String path) {
		int lastDot = path.lastIndexOf('.');
		if (lastDot < 0 || lastDot == path.length() - 1) {
			return "";
		}
		return path.substring(lastDot + 1);
	}

	// ==================== STORAGE RESOLUTION ====================

	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			if (grs.get(0) instanceof String) {
				return Utility.getStorage((String) grs.get(0));
			}
			return (IStorageEngine) grs.get(0);
		}

		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if (storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}

		throw new NullPointerException("No storage engine defined");
	}

	// ==================== DESCRIPTIONS ====================

	@Override
	public String getReactorDescription() {
		return "Reads a file from storage into memory as a byte array and returns as base64. "
				+ "Optionally converts DOCX, DOC, or XLSX to PDF using Playwright before returning.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance or id";
		} else if (key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The path to the file in storage to read into memory";
		} else if (key.equals(CONVERT_TO_PDF_KEY)) {
			return "If true, converts DOCX/DOC/XLSX to PDF via Playwright before returning. "
					+ "PDF and TXT pass through unchanged. PPTX is returned as-is (unsupported).";
		}
		return super.getDescriptionForKey(key);
	}
}