package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		List<Map<String, Object>> results = new ArrayList<>();

		ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
		ZipEntry entry;

		while ((entry = zis.getNextEntry()) != null) {

			File temp = new File("temp_" + entry.getName());
			Files.copy(zis, temp.toPath());

			results.add(FileExtractorEngine.process(temp));
		}

		return Map.of("data", results, "rawText", results.toString());
	}
}
