package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;

public class DocxExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		XWPFDocument doc = new XWPFDocument(new FileInputStream(file));
		XWPFWordExtractor extractor = new XWPFWordExtractor(doc);

		String text = extractor.getText();

		return Map.of("data", text, "rawText", text);
	}
}
