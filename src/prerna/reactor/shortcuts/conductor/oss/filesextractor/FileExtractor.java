package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.util.Map;

public interface FileExtractor {
	Map<String, Object> extract(File file) throws Exception;
}
