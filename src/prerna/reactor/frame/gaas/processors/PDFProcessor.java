package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class PDFProcessor extends AbstractFileProcessor {

	private static final Logger classLogger = LogManager.getLogger(PDFProcessor.class);

	public PDFProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		super(filePath, writer);
	}

	@Override
	public void process() throws IOException {
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
				this.writer.writeRow(source, pageIndex+"", parsedText);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
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

}
