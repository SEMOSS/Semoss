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
package prerna.reactor.database;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUsageLimitUtils;
import prerna.auth.utils.SecurityEngineUsageLimitUtils.Entry;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.GsonUtility;

/**
 * Owner policies on a database, or admin policies across databases. Does not
 * grant access.
 */
public class SetDatabaseUsageLimitReactor extends AbstractReactor {
	private static final Logger LOG = LogManager
			.getLogger(SetDatabaseUsageLimitReactor.class);

	public SetDatabaseUsageLimitReactor() {
		keysToGet = new String[] { ReactorKeysEnum.CONFIG.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		var values = store.getGenRowStruct(ReactorKeysEnum.CONFIG.getKey());
		if (values == null || values.isEmpty())
			throw new IllegalArgumentException("Must provide a usage policy config");
		Object value = values.get(0);
		var gson = GsonUtility.getDefaultGson();
		Entry request;
		try {
			request = value instanceof String json ? gson.fromJson(json, Entry.class)
					: gson.fromJson(gson.toJson(value), Entry.class);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Invalid usage policy config");
		}
		if (request == null || request.key() == null)
			throw new IllegalArgumentException("Must provide the usage policy key");
		if (request.key().catalog() != IEngine.CATALOG_TYPE.DATABASE)
			throw new IllegalArgumentException("This operation configures database usage only");
		try {
			var actor = insight.getUser();
			// Admin authorization is proven by getInstance, never by request JSON.
			var admin = SecurityAdminUtils.getInstance(actor);
			if (admin != null)
				SecurityEngineUsageLimitUtils.setAdminLimit(admin, actor, request.key(), request.usageLimit());
			else
				SecurityEngineUsageLimitUtils.setLimit(actor, request.key(), request.usageLimit());
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			LOG.error("Unable to save database usage policy for source {}", request.key().source(), e);
			throw new SemossPixelException("Unable to save the database usage policy");
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("key", request.key());
		result.put("usageLimit", request.usageLimit());
		return new NounMetadata(result, PixelDataType.MAP);
	}
}
