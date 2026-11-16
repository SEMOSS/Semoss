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
package prerna.auth.utils.reactors.admin;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;

@Deprecated
public class AdminAddBlockReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminAddBlockReactor.class);

	@Deprecated
	public AdminAddBlockReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.SECTION.getKey(),
				ReactorKeysEnum.JSON.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Deprecated
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (!isAdmin) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();
		Map<String, Object> blockDetails = getBlockDetails();
		String blockId = BlocksThemeUtils.addBlock(blockDetails);
		NounMetadata nm = new NounMetadata(blockId, PixelDataType.CONST_STRING);
		return nm;
	}

	// prepares map from inputs fields for use in block creation logic
	private Map<String, Object> getBlockDetails() {
		Map<String, Object> blockMap = new HashMap<>();
		blockMap.put("name", keyValue.get(ReactorKeysEnum.NAME.getKey()));
		blockMap.put("section", keyValue.get(ReactorKeysEnum.SECTION.getKey()));

		String rawJson = keyValue.get(ReactorKeysEnum.JSON.getKey());
		// Removes <encode> wrapper for json field
		if (rawJson != null) {
			try {
				rawJson = URLDecoder.decode(rawJson, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				classLogger.error("Failed to decode the JSON of block {}",
						this.keyValue.get(ReactorKeysEnum.NAME.getKey()), e);
			}
		}
		blockMap.put("json", rawJson);
		blockMap.put("created_by", this.insight.getUserId());
		return blockMap;
	}

	@Deprecated
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Admin Only: Adds a block to the blocks library and returns the new block's id")
				.key(ReactorKeysEnum.NAME, "The block's name")
				.key(ReactorKeysEnum.SECTION, "The section of the blocks library that lists the block")
				.key(ReactorKeysEnum.JSON, "The block's definition as JSON")
				.output(String.class, "The id of the new block");
	}
}
