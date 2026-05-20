/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
