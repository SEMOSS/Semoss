package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;

public class FileTypeDetector {
	public static String detect(File file) throws Exception {

		String name = file.getName().toLowerCase();

		if (name.endsWith(".json")) {
			return "JSON";
		}
		if (name.endsWith(".csv")) {
			return "CSV";
		}
		if (name.endsWith(".xml")) {
			return "XML";
		}
		if (name.endsWith(".txt")) {
			return "TXT";
		}
		if (name.endsWith(".pdf")) {
			return "PDF";
		}
		if (name.endsWith(".docx")) {
			return "DOCX";
		}
		if (name.endsWith(".xlsx")) {
			return "XLSX";
		}
		if (name.endsWith(".pptx")) {
			return "PPTX";
		}
		if (name.endsWith(".jpg") || name.endsWith(".png")) {
			return "IMAGE";
		}
		if (name.endsWith(".zip")) {
			return "ZIP";
		}

		return "UNKNOWN";
	}
}
