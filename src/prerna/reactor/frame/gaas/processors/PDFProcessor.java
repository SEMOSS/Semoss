package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
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
		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(new File(this.filePath)))) {
			String source = getSource(this.filePath);
			PDFTextStripper pdfStripper = new PDFTextStripper();
			for (int pageIndex = 1; pageIndex <= document.getNumberOfPages(); pageIndex++) {
				// stripper is 1 based
				pdfStripper.setStartPage(pageIndex);
				pdfStripper.setEndPage(pageIndex);
				String parsedText = pdfStripper.getText(document);
				this.writer.writeRow(source, pageIndex+"", parsedText);
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}				// stripper is 1 based

	}	

}
