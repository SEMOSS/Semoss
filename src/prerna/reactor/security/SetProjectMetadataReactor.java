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
package prerna.reactor.security;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.playwright.MakePlaywrightMCPReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class SetProjectMetadataReactor extends AbstractSetMetadataReactor {
	
	public SetProjectMetadataReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(), META, 
				ReactorKeysEnum.ENCODED.getKey(), ReactorKeysEnum.JSON_CLEANUP.getKey()
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
		}
		
		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityProjectUtils.getAllMetakeys();
		if(!validMetakeys.containsAll(metadata.keySet())) {
	    	throw new IllegalArgumentException("Unallowed metakeys. Can only use: "+String.join(", ", validMetakeys));
		}
		
		SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		initializePlaywrightAssetsIfTagged(projectId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the project"));
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Define metadata on a project";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the database. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}

	private void initializePlaywrightAssetsIfTagged(String projectId, Map<String, Object> metadata) {
		if(metadata == null || metadata.isEmpty()) {
			return;
		}
		Object rawTags = metadata.get("tag");
		if(rawTags == null) {
			return;
		}

		Set<String> normalized = new HashSet<>();
		if(rawTags instanceof Iterable<?>) {
			for(Object candidate : (Iterable<?>) rawTags) {
				if(candidate == null) {
					continue;
				}
				String value = candidate.toString().trim();
				if(!value.isEmpty()) {
					normalized.add(value.toUpperCase());
				}
			}
		} else if(rawTags.getClass().isArray()) {
			Object[] arr = (Object[]) rawTags;
			for(Object candidate : arr) {
				if(candidate == null) {
					continue;
				}
				String value = candidate.toString().trim();
				if(!value.isEmpty()) {
					normalized.add(value.toUpperCase());
				}
			}
		} else {
			String value = rawTags.toString().trim();
			if(!value.isEmpty()) {
				normalized.add(value.toUpperCase());
			}
		}

		if(normalized.contains("PLAYWRIGHT") && normalized.contains("MCP")) {
			MakePlaywrightMCPReactor.regenerateForProject(this.insight, projectId,
					"add: Auto-generated via SetProjectMetadataReactor");
		}
	}

}
