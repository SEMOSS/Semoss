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
package prerna.reactor.qs;

import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Utility;

public abstract class AbstractSparqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractSparqlQueryReactor.class);

	private static final int DEFAULT_LIMIT = 50;
	private static final int MAX_LIMIT = 5_000;

	public AbstractSparqlQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), "raw" };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to run this operation",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		organizeKeys();
		String sparqlQuery = getDecodedQuery();
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		String limitStr = this.keyValue.get(this.keysToGet[2]);
		String rawStr = this.keyValue.get(this.keysToGet[3]);

		if (sparqlQuery == null || sparqlQuery.trim().isEmpty()) {
			throw new SemossPixelException("SPARQL query cannot be empty");
		}

		if (databaseId == null || databaseId.trim().isEmpty()) {
			throw new SemossPixelException("Database id is required");
		}

		if (!isSparqlSelect(sparqlQuery)) {
			throw new SemossPixelException("Only SPARQL SELECT queries are supported");
		}

		IEngine engine = Utility.getEngine(databaseId);
		if (!(engine instanceof IRDFDatabase)) {
			throw new IllegalArgumentException("The database is not an RDF engine that accepts SPARQL");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
			throw new SemossPixelException("User does not have permission to query this database");
		}

		boolean raw = rawStr != null && Boolean.parseBoolean(rawStr.trim());

		try {
			HardSelectQueryStruct qs = buildQs(sparqlQuery, (IDatabaseEngine) engine, databaseId);
			int limit = parseLimit(limitStr);
			BasicIteratorTask task = new BasicIteratorTask(qs);
			task.setNumCollect(limit);
			task.setCollectLimit(limit);
			task.setReturnRaw(raw);
			this.insight.addQueriedDatabases(databaseId);
			return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error executing SPARQL query for database {}", databaseId, e);
			throw new SemossPixelException("Error executing SPARQL query: " + e.getMessage());
		}
	}

	/**
	 * @return Decoded SPARQL query string ready for execution
	 */
	protected abstract String getDecodedQuery();

	/**
	 * Skips PREFIX/BASE declarations and comment lines, then checks if the first
	 * real query keyword is SELECT.
	 */
	private static boolean isSparqlSelect(String sparql) {
		for (String line : sparql.split("\\n")) {
			String t = line.trim();
			if (t.isEmpty() || t.startsWith("#")) {
				continue;
			}
			String upper = t.toUpperCase(Locale.ROOT);
			if (upper.startsWith("PREFIX ") || upper.startsWith("BASE ")) {
				continue;
			}
			return upper.startsWith("SELECT");
		}
		return false;
	}

	private HardSelectQueryStruct buildQs(String sparqlQuery, IDatabaseEngine engine, String databaseId) {
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setEngineId(databaseId);
		qs.setEngine(engine);
		qs.setQuery(sparqlQuery);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return qs;
	}

	private int parseLimit(String limitStr) {
		int limit = DEFAULT_LIMIT;
		if (limitStr != null && !limitStr.trim().isEmpty()) {
			try {
				limit = Integer.parseInt(limitStr.trim());
				if (limit <= 0) {
					classLogger.warn("Non-positive limit value: {}, using default {}", limit, DEFAULT_LIMIT);
					limit = DEFAULT_LIMIT;
				} else if (limit > MAX_LIMIT) {
					classLogger.warn("Limit value {} exceeds maximum {}, using maximum", limit, MAX_LIMIT);
					limit = MAX_LIMIT;
				}
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid limit value: {}, using default {}", limitStr, DEFAULT_LIMIT);
			}
		}
		return limit;
	}

	@Override
	public String getReactorDescription() {
		return "Execute a SPARQL SELECT query against an RDF database";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Limits the number of rows retrieved";
		} else if (key.equals("raw")) {
			return "When true, returns the full URI or literal representation for each binding instead of the processed instance name";
		}
		return super.getDescriptionForKey(key);
	}

}
