package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

public class TxtExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		String content = Files.readString(file.toPath());

		return Map.of("data", content, "rawText", content);
	}
}
