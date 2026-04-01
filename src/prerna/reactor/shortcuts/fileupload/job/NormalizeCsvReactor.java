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
package prerna.reactor.shortcuts.fileupload.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.interceptor.PipelineReactorUtils;
import prerna.reactor.interceptor.ReactorInputHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NormalizeCsvReactor extends AbstractReactor {

	private String schema; // AUTO (future use)

	/*
	 * public NormalizeCsvAction(Map<String, Object> config) { this.schema = config
	 * != null ? String.valueOf(config.getOrDefault("schema", "AUTO")) : "AUTO"; }
	 */

	@Override
	public NounMetadata execute() {

		ReactorInputHelper helper = new ReactorInputHelper(this.getNounStore());
		// File file = helper.getConfigParameter("input", File.class);
		Map<String, Object> result = new HashMap<String, Object>();
		Map<String, Object> config = helper.getConfigParameter(PipelineReactorUtils.CONFIG, Map.class);
		Map<String, Object> arguments = helper.getArgumentsMap();

		Object extractionObj = arguments.get("data");
		this.schema = (String) config.get("schema");

		if (!(extractionObj instanceof Map<?, ?> extraction)) {
			throw new IllegalStateException("CSV normalization failed: extraction data missing");
		}

		String rawText = (String) extraction.get("rawText");

		if (rawText == null || rawText.isBlank()) {
			throw new IllegalStateException("CSV normalization failed: rawText is empty");
		}

		String[] lines = rawText.split("\\R");
		if (lines.length < 2) {
			throw new IllegalStateException("CSV normalization failed: not enough rows");
		}

		// Parse headers
		String[] headers = lines[0].split(",");

		// Parse rows
		List<Map<String, String>> rows = new ArrayList<>();

		for (int i = 1; i < lines.length; i++) {
			String[] values = lines[i].split(",", -1);

			Map<String, String> row = new LinkedHashMap<>();
			for (int j = 0; j < headers.length; j++) {
				String key = headers[j].trim();
				String value = j < values.length ? values[j].trim() : "";
				row.put(key, value);
			}
			rows.add(row);
		}

		// Build normalized JSON
		Map<String, Object> normalized = new LinkedHashMap<>();
		normalized.put("type", "CSV");
		normalized.put("headers", Arrays.asList(headers));
		normalized.put("rows", rows);
		normalized.put("rowCount", rows.size());

		// Store in context
		result.put("data", normalized);

		// Optional metadata
		result.put("meta", "CSV");

		System.out.println(" CSV normalized. Rows=" + rows.size());
		return new NounMetadata(result, PixelDataType.MAP);

	}

}
