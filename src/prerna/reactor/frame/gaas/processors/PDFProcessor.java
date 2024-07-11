package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.util.Constants;
import prerna.util.Utility;

public class PDFProcessor {

	private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);

	// constructor with file name
	// For every slide get the text shapes
	// index it into a csv
	private String filePath = null;
	private CSVWriter writer = null;

	public PDFProcessor(String filePath, CSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}

	/**
	 * 
	 */
	public void process() {
		PDDocument pdDoc = null;
		try {
			File f = new File(this.filePath);
			String source = getSource(this.filePath);
			PDFTextStripper pdfStripper = new PDFTextStripper();
			pdDoc = PDDocument.load(f);
			int totalPages = pdDoc.getNumberOfPages();
			for(int pageIndex = 1;pageIndex <= totalPages;pageIndex++)
			{
				pdfStripper.setStartPage(pageIndex);
				pdfStripper.setEndPage(pageIndex);
				String parsedText = pdfStripper.getText(pdDoc);
				writer.writeRow(source, pageIndex+"", parsedText, "");
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
