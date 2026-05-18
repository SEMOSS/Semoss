package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonExtractor implements FileExtractor {
	@Override
	public Map<String, Object> extract(File file) throws Exception {

		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> data = mapper.readValue(file, Map.class);

		return Map.of("data", data, "rawText", data);
	}
}
