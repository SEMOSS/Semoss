package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class OCRProcessor {

	private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);
	private String filePath = null;
	private VectorDatabaseCSVWriter writer = null;

	public OCRProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}

	public void process(List<String> result,String type) {
            //If file type is PDF
			if(type.equalsIgnoreCase("pdf")) {
				try {
					processPdf(result);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to process pdf file");
				}
			
			}
	}
	

	
	public void processPdf(List<String> result) throws IOException {
		String source = getSource(this.filePath);
		PDFTextStripper pdfStripper = new PDFTextStripper();
		if (result.size() > 0) {
			for (int pageIndex = 0; pageIndex < result.size(); pageIndex++) {
				pdfStripper.setStartPage(pageIndex);
				pdfStripper.setEndPage(pageIndex);
				System.out.println(result.get(pageIndex) + ":" + pageIndex);
				writer.writeRow(source, pageIndex + "", result.get(pageIndex), "");

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
		if (file.exists()) {
			source = file.getName();
		}
		return source;
	}

}