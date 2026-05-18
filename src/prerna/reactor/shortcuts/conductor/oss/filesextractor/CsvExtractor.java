package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvExtractor implements FileExtractor {
	@Override
	public Map<String, Object> extract(File file) throws Exception {

		List<Map<String, Object>> rows = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			String headerLine = br.readLine();
			String[] headers = headerLine.split(",");

			String line;
			while ((line = br.readLine()) != null) {

				String[] values = line.split(",");
				Map<String, Object> row = new HashMap<>();

				for (int i = 0; i < headers.length; i++) {
					row.put(headers[i], values[i]);
				}

				rows.add(row);
			}
		}

		return Map.of("data", rows, "rawText", rows.toString());
	}
}
