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
package prerna.reactor.shortcuts.fileupload.job;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.tika.Tika;
import org.w3c.dom.Document;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FileExtractReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(FileExtractReactor.class);
	private static final Tika tika = new Tika();

	/*
	 * private final boolean storeRawText; private final boolean
	 * storeStructuredData; private final ObjectMapper mapper = new ObjectMapper();
	 */

	/*
	 * public FileExtractReactor(Map<String, Object> config) { this.storeRawText =
	 * Boolean.TRUE.equals(config.get("storeRawText")); this.storeStructuredData =
	 * Boolean.TRUE.equals(config.get("storeStructuredData")); }
	 */

	public FileExtractReactor() {
		// No keysToGet needed as we use ReactorInputHelper
		this.keysToGet = new String[] { ReactorKeysEnum.CONFIG.getKey(), ReactorKeysEnum.INPUT.getKey(),
				ReactorKeysEnum.RESULT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			organizeKeys();
			Map<String, Object> config = getConfigMap();

			NounMetadata nounMetadata = null;
			String keyPath = null;

			Map<String, Object> result = new HashMap<String, Object>();
			// Map<String, Object> inputMap = getResultMap();

			// ReactorInputHelper helper = new ReactorInputHelper(this.getNounStore());

			String filePath = this.keyValue.get(ReactorKeysEnum.INPUT.getKey());
			/*
			 * URI uri = URI.create(filePath); Path path = Paths.get(uri);
			 */
			// String mimeType = tika.detect(path.toFile());
			Path path = Path.of(filePath);
			File file = new File(filePath);
			// Map<String, Object> param = helper.getConfigParameter("config", Map.class);

			// File file = (File) ctx.input.get("file");
			String name = file.getName().toLowerCase();
			// WorkflowActionResult workflowActionResult = new WorkflowActionResult();
			FileExtractionResult r = new FileExtractionResult();
			r.fileName = file.getName();
			r.mimeType = tika.detect(file);

			if (name.endsWith(".pdf")) {
				r.fileType = "PDF";
				if (Boolean.TRUE.equals(config.get("storeRawText"))) {
					r.extractedText = extractPdf(file);
				}
			} else if (name.endsWith(".docx") || name.endsWith(".doc")) {
				r.fileType = "DOCX";
				if (Boolean.TRUE.equals(config.get("storeRawText"))) {
					r.extractedText = extractDoc(file);
				}

			} else if (name.endsWith(".xls") || name.endsWith(".xlsx")) {
				r.fileType = "XLSX";
				if (Boolean.TRUE.equals(config.get("storeStructuredData"))) {
					r.structuredData = extractExcel(file);
				}
			} else if (name.endsWith(".csv")) {
				r.fileType = "CSV";
				if (Boolean.TRUE.equals(config.get("storeStructuredData"))) {
					r.structuredData = extractCsv(file);
				}

			} else if (name.endsWith(".json")) {
				r.fileType = "JSON";
				ObjectMapper mapper = new ObjectMapper();
				if (Boolean.TRUE.equals(config.get("storeStructuredData"))) {
					r.structuredData = mapper.readTree(file);
				}
				if (Boolean.TRUE.equals(config.get("storeRawText"))) {
					r.extractedText = r.structuredData.toString();
				}

			} else if (name.endsWith(".xml")) {
				r.fileType = "XML";
				Document doc = null;
				if (Boolean.TRUE.equals(config.get("storeStructuredData"))) {
					DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					doc = builder.parse(file);
					r.structuredData = doc;
				}
				if (Boolean.TRUE.equals(config.get("storeRawText"))) {
					r.extractedText = doc.getDocumentElement().getTextContent();
				}
			} else if (r.mimeType.startsWith("image/")) {
				/*
				 * r.fileType = "IMAGE"; Tesseract tesseract = new Tesseract(); r.extractedText
				 * = tesseract.doOCR(file);
				 */

			} else {
				r.fileType = "TXT";
				if (Boolean.TRUE.equals(config.get("storeRawText"))) {
					r.extractedText = Files.readString(path);
				}
			}
			// workflowActionResult.result.put("FileExtractionResult", r);

			System.out.println("Extracted fileType=" + r.fileType);

			String resultKey = (String) config.get("resultKey");

			result.put(resultKey, r);

			/*
			 * Map<String, Object> variable = (Map<String, Object>) config.get("varStore");
			 * 
			 * Map<String, String> output = (Map<String, String>) variable.get("output");
			 * 
			 * for (Map.Entry<String, String> entry : output.entrySet()) {
			 * 
			 * keyPath = entry.getKey(); // results.action1 String expression =
			 * entry.getValue(); // ${result}
			 * 
			 * Object value = resolveExpression(expression, r); result.put(keyPath, value);
			 * 
			 * }
			 */

			nounMetadata = new NounMetadata(result, PixelDataType.MAP);
			// planner.addVariable(resultKey, nounMetadata);
			return nounMetadata;
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		// ctx.data.put("extraction", r);
		return null;

	}

	private String extractPdf(File file) throws Exception {
		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(new File(file.getAbsolutePath())))) {
			return new PDFTextStripper().getText(document);
		}
	}

	private String extractDoc(File file) throws Exception {
		try (XWPFDocument doc = new XWPFDocument(new FileInputStream(file))) {
			StringBuilder sb = new StringBuilder();
			for (XWPFParagraph p : doc.getParagraphs()) {
				sb.append(p.getText()).append("\n");
			}
			return sb.toString();
		}
	}

	private List<Map<String, String>> extractExcel(File file) throws Exception {
		Workbook wb = WorkbookFactory.create(file);
		Sheet sheet = wb.getSheetAt(0);

		Row header = sheet.getRow(0);
		List<Map<String, String>> rows = new ArrayList<>();

		for (int i = 1; i <= sheet.getLastRowNum(); i++) {
			Row row = sheet.getRow(i);
			Map<String, String> map = new HashMap<>();
			for (int j = 0; j < header.getLastCellNum(); j++) {
				map.put(header.getCell(j).getStringCellValue(), row.getCell(j).toString());
			}
			rows.add(map);
		}
		return rows;
	}

	private List<String[]> extractCsv(File file) throws Exception {
		List<String[]> rows = new ArrayList<>();
		for (String line : Files.readAllLines(file.toPath())) {
			rows.add(line.split(","));
		}
		return rows;
	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getResultMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.RESULT.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getConfigMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.CONFIG.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	public Object resolveExpression(String expression, Object workflowResult) {

		if ("${result}".equals(expression)) {

			return workflowResult;
		}

		return expression;
	}
}
