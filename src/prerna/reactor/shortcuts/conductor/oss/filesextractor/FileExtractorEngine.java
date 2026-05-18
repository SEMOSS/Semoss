package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class FileExtractorEngine {
	public static Map<String, Object> process(File file) throws Exception {

		String type = FileTypeDetector.detect(file);
		Map<String, Object> result = new HashMap<>(ExtractorFactory.get(type).extract(file));
		result.put("fileType", type);
		result.put("fileName", file.getName());

		return result;
	}
}
