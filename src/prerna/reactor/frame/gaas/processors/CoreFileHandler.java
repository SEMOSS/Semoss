package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;

public class CoreFileHandler extends AbstractFileHandler {
	public static final Set<String> SUPPORTED_TYPES = Set.of("csv", "doc", "docx", "json", "pdf", "ppt", "pptx", "txt",
			"xml", "rtf");

	@Override
	public boolean supportsFile(File file) {
		String fileType = FilenameUtils.getExtension(file.getAbsolutePath());
		return SUPPORTED_TYPES.contains(fileType);
	}

	@Override
	public IFileProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer) {
		String fileType = FilenameUtils.getExtension(file.getAbsolutePath());
		String filePath = file.getAbsolutePath();
		String mimeType = this.getMimeType(file);
		IFileProcessor subprocessor;
		switch (fileType) {
		case "doc":
		case "docx":
			if (mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
					|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
							|| mimeType.equalsIgnoreCase("application/msword")
							|| mimeType.equalsIgnoreCase("application/x-tika-msoffice")))) {
				subprocessor = new DocProcessor(filePath, writer);
				break;
			}
		case "pdf":
			if (mimeType.equalsIgnoreCase("application/pdf")) {
				subprocessor = new PDFProcessor(filePath, writer);
				break;
			}
		case "ppt":
		case "pptx":
			if (mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
					|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
							|| (mimeType.equalsIgnoreCase("application/vnd.ms-powerpoint"))))) {
				subprocessor = new PPTProcessor(filePath, writer);
				break;
			}
		case "csv":
		case "json":
		case "txt":
		case "xml":
		case "rtf":
			subprocessor = new TextFileProcessor(filePath, writer);
			break;
		default:
			subprocessor = null;
		}
		return subprocessor;
	}
}