package prerna.reactor.shortcuts.conductor.oss.filesextractor;

public class ExtractorFactory {
	public static FileExtractor get(String type) {

		switch (type) {
		case "JSON":
			return new JsonExtractor();
		case "CSV":
			return new CsvExtractor();
		case "TXT":
			return new TxtExtractor();
		case "XML":
			return new XmlExtractor();
		case "PDF":
			return new PdfExtractor();
		case "DOCX":
			return new DocxExtractor();
		case "XLSX":
			return new ExcelExtractor();
		case "IMAGE":
			return new ImageExtractor();
		case "ZIP":
			return new ZipExtractor();
		default:
			throw new RuntimeException("Unsupported type");
		}
	}
}
