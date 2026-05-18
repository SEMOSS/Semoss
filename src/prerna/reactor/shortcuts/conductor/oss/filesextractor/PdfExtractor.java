package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.util.Map;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class PdfExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		try (PDDocument document = Loader.loadPDF(new RandomAccessReadBufferedFile(new File(file.getAbsolutePath())))) {
			PDFTextStripper stripper = new PDFTextStripper();

			String text = stripper.getText(document);
			document.close();

			return Map.of("data", text, "rawText", text);
		}

	}
}
