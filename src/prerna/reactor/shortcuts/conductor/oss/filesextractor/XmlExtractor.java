package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

public class XmlExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		String xml = Files.readString(file.toPath());

		return Map.of("data", xml, "rawText", xml);
	}
}
