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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;

class AutomationDataExtractExecutorUnitTests {

	@Test
	void extractsJsonWithDistinctNullAndMissingFallbacks() {
		Map<String, Object> config = config("{\"items\":[{\"value\":null}]}", "$.items[0].value", "json");
		config.put(AutomationConstants.CONFIG_NULL_VALUE, "was-null");
		config.put(AutomationConstants.CONFIG_MISSING_VALUE, "was-missing");

		assertEquals("was-null", AutomationDataExtractExecutor.execute(config, Map.of(), "extract", path -> null));
		config.put(AutomationConstants.CONFIG_PATH, "/items/1/value");
		assertEquals("was-missing", AutomationDataExtractExecutor.execute(config, Map.of(), "extract", path -> null));
	}

	@Test
	void extractsBase64AndGovernedAssetXml() {
		String encoded = Base64.getEncoder().encodeToString(
				"{\"message\":\"quotes \\\" ; unicode \u03bb\"}".getBytes(StandardCharsets.UTF_8));
		assertEquals("quotes \" ; unicode \u03bb",
				AutomationDataExtractExecutor.execute(config(encoded, "/message", "auto"), Map.of(), "extract",
						path -> null));

		Map<String, Object> assetConfig = config("/input/data.xml", "$.root.item[1]", "xml");
		assertEquals("second", AutomationDataExtractExecutor.execute(assetConfig, Map.of(), "extract",
				path -> "<root><item>first</item><item>second</item></root>"));
	}

	@Test
	void rejectsXmlExternalEntitiesAndInvalidPaths() {
		Map<String, Object> xml = config(
				"<!DOCTYPE root [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><root>&xxe;</root>",
				"$.root", "xml");
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDataExtractExecutor.execute(xml, Map.of(), "extract", path -> {
					throw new IllegalArgumentException("not an asset");
				}));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDataExtractExecutor.execute(config("{\"a\":1}", "$bad", "json"), Map.of(),
						"extract", path -> null));
	}

	@Test
	void extractsAnExplicitScalarFromAFrame() throws Exception {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
		when(frame.size("query")).thenReturn(1L);
		when(frame.getQsHeaders()).thenReturn(new String[] { "QUERY__ID", "QUERY__STATUS" });
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID", "STATUS" });
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(frame.query(any(SelectQueryStruct.class))).thenReturn(wrapper);
		when(wrapper.hasNext()).thenReturn(true, false);
		when(wrapper.next()).thenReturn(
				new HeadersDataRow(new String[] { "ID", "STATUS" }, new Object[] { 7, "ready" }));
		AutomationFrameOutput.RegisteredFrame output =
				AutomationFrameOutput.register(new prerna.om.Insight(), "query", frame);

		assertEquals("ready", AutomationDataExtractExecutor.execute(
				config("${query}", "$[0].STATUS", "json"), Map.of("query", output), "extract", path -> null));
	}

	private static Map<String, Object> config(Object source, String path, String format) {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(AutomationConstants.CONFIG_SOURCE, source);
		config.put(AutomationConstants.CONFIG_PATH, path);
		config.put(AutomationConstants.CONFIG_FORMAT, format);
		config.put(AutomationConstants.CONFIG_MISSING_VALUE, null);
		config.put(AutomationConstants.CONFIG_NULL_VALUE, null);
		return config;
	}
}
