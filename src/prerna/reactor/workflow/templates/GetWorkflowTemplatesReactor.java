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
package prerna.reactor.workflow.templates;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the list of available workflow templates.
 *
 * <p>Pixel: {@code GetWorkflowTemplates()}
 *
 * <p>Templates are bundled as a JSON resource in the classpath. Each template
 * includes metadata (id, name, description, category, icon) and the full
 * workflow graph definition that can be used to create a new workflow.
 */
public class GetWorkflowTemplatesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetWorkflowTemplatesReactor.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	private static final String TEMPLATES_RESOURCE = "prerna/reactor/workflow/templates/workflow-templates.json";

	// Cached templates (loaded once)
	private static volatile List<Map<String, Object>> cachedTemplates;

	public GetWorkflowTemplatesReactor() {
		this.keysToGet = new String[]{};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<Map<String, Object>> templates = loadTemplates();

		// Return summary info only (not the full workflow graph)
		List<Map<String, Object>> summaries = new ArrayList<>();
		for (Map<String, Object> template : templates) {
			Map<String, Object> summary = new HashMap<>();
			summary.put("id", template.get("id"));
			summary.put("name", template.get("name"));
			summary.put("description", template.get("description"));
			summary.put("category", template.get("category"));
			summary.put("icon", template.get("icon"));
			summary.put("configKeys", template.get("configKeys"));
			summaries.add(summary);
		}

		return new NounMetadata(summaries, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	/**
	 * Loads and returns the full template by ID. Used internally by
	 * {@link CreateWorkflowFromTemplateReactor}.
	 */
	@SuppressWarnings("unchecked")
	static Map<String, Object> getTemplateById(String templateId) {
		List<Map<String, Object>> templates = loadTemplates();
		for (Map<String, Object> t : templates) {
			if (templateId.equals(t.get("id"))) {
				return t;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> loadTemplates() {
		if (cachedTemplates != null) {
			return cachedTemplates;
		}
		synchronized (GetWorkflowTemplatesReactor.class) {
			if (cachedTemplates != null) {
				return cachedTemplates;
			}
			try (InputStream is = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(TEMPLATES_RESOURCE)) {
				if (is == null) {
					classLogger.warn("Workflow templates resource not found: {}", TEMPLATES_RESOURCE);
					cachedTemplates = new ArrayList<>();
					return cachedTemplates;
				}
				InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
				cachedTemplates = GSON.fromJson(reader, new TypeToken<List<Map<String, Object>>>() {}.getType());
				if (cachedTemplates == null) {
					cachedTemplates = new ArrayList<>();
				}
				classLogger.info("Loaded {} workflow templates", cachedTemplates.size());
			} catch (Exception e) {
				classLogger.error("Failed to load workflow templates: {}", e.getMessage(), e);
				cachedTemplates = new ArrayList<>();
			}
			return cachedTemplates;
		}
	}
}
