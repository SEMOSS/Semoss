package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;
import prerna.util.Utility;

public class PDFProcessor {

	private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	private String filePath = null;
	private VectorDatabaseCSVWriter writer = null;

	public PDFProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}

	/**
	 * 
	 */
	public void process(String ocrEngineId) {
		PDDocument pdDoc = null;
		try {
			File f = new File(this.filePath);
			String source = getSource(this.filePath);
			PDFTextStripper pdfStripper = new PDFTextStripper();
			pdDoc = PDDocument.load(f);
			int totalPages = pdDoc.getNumberOfPages();
			PDFRenderer pdfRenderer = new PDFRenderer(pdDoc);
			
			if (pdfRenderer.renderImage(0) != null && (ocrEngineId != null)) {

				// Call OCRFunction

				IFunctionEngine functionEngine = Utility.getFunctionEngine(ocrEngineId);
				System.out.println("FunctionName:" + functionEngine.getEngineName());
				if (functionEngine == null) {
					throw new IllegalArgumentException("Unable to find engine");
				}
				Map<String, Object> map = new HashMap<>();
				map.put("filePath", this.filePath);
				Object parsedText = functionEngine.execute(map);
				System.out.println("parsedData*********" + parsedText);
				List<String> result = (List<String>) parsedText;

				for (int pageIndex = 0; pageIndex < result.size(); pageIndex++) {
					pdfStripper.setStartPage(pageIndex);
					pdfStripper.setEndPage(pageIndex);
					System.out.println(result.get(pageIndex) + ":" + pageIndex);
					writer.writeRow(source, pageIndex + "", result.get(pageIndex), "");

				}

			} else
				for (int pageIndex2 = 1; pageIndex2 <= totalPages; pageIndex2++) {
					pdfStripper.setStartPage(pageIndex2);
					pdfStripper.setEndPage(pageIndex2);
					String parsedText = pdfStripper.getText(pdDoc);
					System.out.println(pageIndex2 + ": " + parsedText);
					writer.writeRow(source, pageIndex2 + "", parsedText, "");
				}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(pdDoc != null) {
				try {
					pdDoc.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}	

	/**
	 * 
	 * @param filePath
	 * @return
	 */
	private String getSource(String filePath) {
		String source = null;
		File file = new File(filePath);
		if(file.exists()) {
			source = file.getName();
		}
//		source = Utility.cleanString(source, true);
		return source;
	}
	
}
