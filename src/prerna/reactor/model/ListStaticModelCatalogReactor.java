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
package prerna.reactor.model;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.StaticBuiltinToolsCatalog;
import prerna.util.StaticModelMetadataCatalog;

public class ListStaticModelCatalogReactor extends AbstractReactor {

	static final String SERVING_PROVIDER_KEY = "servingProvider";

	/**
	 * Serving hosts the Add Model page has an import form for, in normalized
	 * form. Azure is deliberately absent - the catalog's pricing never lists it
	 * as a host and Azure imports are deployment-name based anyway.
	 */
	private static final Set<String> DEFAULT_SERVING_PROVIDERS = new LinkedHashSet<>(
			List.of("openai", "anthropic", "google", "bedrock", "nvidia", "perplexity"));

	public ListStaticModelCatalogReactor() {
		this.keysToGet = new String[] { SERVING_PROVIDER_KEY };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Set<String> hosts = DEFAULT_SERVING_PROVIDERS;
		String servingProvider = this.keyValue.get(SERVING_PROVIDER_KEY);
		if (servingProvider != null && !servingProvider.trim().isEmpty()) {
			String normalized = StaticBuiltinToolsCatalog.normalizeProviderKey(servingProvider);
			if (normalized == null) {
				throw new IllegalArgumentException("Unrecognized serving provider: " + servingProvider);
			}
			hosts = new LinkedHashSet<>(List.of(normalized));
		}

		Map<String, List<Map<String, Object>>> modelsByHost = StaticModelMetadataCatalog
				.listImportableModels(getMetadataFile(), hosts);
		return new NounMetadata(modelsByHost, PixelDataType.MAP);
	}

	Path getMetadataFile() {
		return StaticModelMetadataCatalog.getMetadataFile();
	}

	@Override
	public String getReactorDescription() {
		return "Lists the meta/model.json catalog models importable through each connectable serving provider, "
				+ "keyed by normalized provider (openai, anthropic, google, bedrock, nvidia, perplexity)";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SERVING_PROVIDER_KEY)) {
			return "Optional serving provider to list models for; omit to list all connectable providers";
		}
		return super.getDescriptionForKey(key);
	}
}
