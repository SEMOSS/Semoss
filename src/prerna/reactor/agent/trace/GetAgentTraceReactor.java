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
package prerna.reactor.agent.trace;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Retrieves a single agent trace by ID.
 *
 * <pre>
 * GetAgentTrace(traceId=["&lt;id&gt;"])
 * </pre>
 */
public class GetAgentTraceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAgentTraceReactor.class);

	private static final String KEY_TRACE_ID = "traceId";

	public GetAgentTraceReactor() {
		this.keysToGet = new String[] { KEY_TRACE_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String traceId = this.keyValue.get(KEY_TRACE_ID);
		if (traceId == null || traceId.isEmpty()) {
			throw new IllegalArgumentException("traceId is required for GetAgentTrace");
		}

		Map<String, Object> trace;
		try {
			trace = AgentTraceLogsUtils.getTrace(traceId);
		} catch (Exception e) {
			classLogger.warn("GetAgentTraceReactor: error fetching trace '{}'.", traceId, e);
			return NounMetadata.getErrorNounMessage("Error retrieving trace: " + traceId);
		}

		if (trace == null) {
			return NounMetadata.getErrorNounMessage("Trace not found: " + traceId);
		}

		return new NounMetadata(trace, PixelDataType.MAP);
	}
}
