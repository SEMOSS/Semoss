package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.util.Map;

public class ImageExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		// Tesseract tesseract = new Tesseract();
		String text = ""; // tesseract.doOCR(file);

		return Map.of("data", text, "rawText", text);
	}
}
