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
package prerna.tcp.client.workers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.IStringExportProcessor;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.tcp.PayloadStruct;
import prerna.util.Utility;

/**
 * Runs a database query that a Python process asks for, as the given user. Any
 * other request from Python runs as a reactor call instead.
 */
public class NativePyEngineWorker implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(NativePyEngineWorker.class);

	private static final String GENERIC_ERROR = "Runtime Error Processing Python Command";
	private static final IStringExportProcessor ESCAPE_QUOTES = input -> input.replace("\"", "\\\"");

	private final PayloadStruct ps;
	private final User user;
	private final Insight insight;
	private PayloadStruct output = null;

	/**
	 * Creates a worker that returns the query results inline as JSON.
	 *
	 * @param user the user the query runs as
	 * @param ps   the request from the Python process
	 */
	public NativePyEngineWorker(User user, PayloadStruct ps) {
		this(user, ps, null);
	}

	/**
	 * Creates a worker that writes the query results to a file in the insight's
	 * folder, unless the request asks for JSON.
	 *
	 * @param user    the user the query runs as
	 * @param ps      the request from the Python process
	 * @param insight the insight whose folder holds the results, or null to return
	 *                them inline
	 */
	public NativePyEngineWorker(User user, PayloadStruct ps, Insight insight) {
		this.ps = ps;
		this.user = user;
		this.insight = insight;
	}

	@Override
	public void run() {
		String engineId = ps.objId;
		try {
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Engine " + engineId + " does not exist or user does not have access to it");
			}
			if (!"DATABASE".equalsIgnoreCase(ps.engineType) || !"execquery".equalsIgnoreCase(ps.methodName)) {
				throw new IllegalArgumentException("Can only execute a database query using this method");
			}
			IDatabaseEngine engine = Utility.getDatabase(engineId);
			try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, ps.payload[0] + "")) {
				if (this.insight == null || (ps.payload.length > 1 && "json".equals(ps.payload[1]))) {
					ps.payload = new Object[] { Utility.writeResultToJsonObject(wrapper, null, ESCAPE_QUOTES) };
				} else {
					String fileLocation = insight.getInsightFolder() + "/a_" + Utility.getRandomString(5) + ".json";
					Utility.writeResultToJson(fileLocation, wrapper, null, ESCAPE_QUOTES);
					ps.payload = new Object[] { fileLocation.replace("\\", "/") };
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to run the Python process's query on engine {}", engineId, e);
			String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getLocalizedMessage();
			ps.ex = errorMessage == null ? GENERIC_ERROR : errorMessage;
		}
		ps.response = true;
		output = ps;
	}

	/**
	 * Returns the request with its results or its error filled in.
	 *
	 * @return the answered request, or null until {@link #run()} finishes
	 */
	public PayloadStruct getOutput() {
		return this.output;
	}
}
