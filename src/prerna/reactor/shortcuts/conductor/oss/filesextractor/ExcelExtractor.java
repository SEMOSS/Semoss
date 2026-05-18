package prerna.reactor.shortcuts.conductor.oss.filesextractor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class ExcelExtractor implements FileExtractor {

	@Override
	public Map<String, Object> extract(File file) throws Exception {

		List<Map<String, Object>> rows = new ArrayList<>();

		Workbook wb = WorkbookFactory.create(file);
		Sheet sheet = wb.getSheetAt(0);

		Row headerRow = sheet.getRow(0);

		for (int i = 1; i <= sheet.getLastRowNum(); i++) {

			Row row = sheet.getRow(i);
			Map<String, Object> map = new HashMap<>();

			for (int j = 0; j < headerRow.getLastCellNum(); j++) {
				map.put(headerRow.getCell(j).toString(), row.getCell(j).toString());
			}

			rows.add(map);
		}

		return Map.of("data", rows, "rawText", rows.toString());
	}
}
