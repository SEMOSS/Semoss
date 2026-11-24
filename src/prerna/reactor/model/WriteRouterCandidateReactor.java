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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineCloneUtility;
import prerna.util.ValueUtils;

/**
 * The only router write the router-optimizer agent has: clones the live router into a new private engine and writes
 * the given config there, without an {@code improvement} section, so a candidate never learns or schedules anything.
 * The live router is only read. Returns the candidate's engine id for {@code EvaluateRouterCandidate} and
 * {@code ApplyRouterCandidate}.
 */
public class WriteRouterCandidateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(WriteRouterCandidateReactor.class);

	private static final String LIVE_KEY = "liveEngineId";
	private static final String MAP_KEY = "map";

	public WriteRouterCandidateReactor() {
		this.keysToGet = new String[] { LIVE_KEY, MAP_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String liveId = ValueUtils.trimToNull(this.keyValue.get(LIVE_KEY));
		UpdateModelRouterConfigReactor.editableRouter(this.insight, liveId);
		// same rule as CloneEngine
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser()) && !SecurityEngineUtils.userIsOwner(this.insight.getUser(), liveId)) {
			throw new IllegalArgumentException("Only the owner of router " + liveId + " can write a candidate of it");
		}
		Map<String, Object> map = this.<String, Object>getGenericMap(MAP_KEY, null);
		if (map == null || map.isEmpty()) {
			throw new IllegalArgumentException("Must provide the candidate routing configuration map");
		}
		JsonObject config = JsonParser.parseString(new Gson().toJson(map)).getAsJsonObject();
		config.remove("improvement");
		String candidateId = EngineCloneUtility.cloneEngine(this.insight.getUser(), liveId,
				SecurityEngineUtils.getEngineDisplayNameForId(liveId) + " candidate " + System.currentTimeMillis(), classLogger);
		UpdateModelRouterConfigReactor.save(this.insight, candidateId, UpdateModelRouterConfigReactor.editableRouter(this.insight, candidateId),
				config.toString(), "WriteRouterCandidate of " + liveId);
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("candidateEngineId", candidateId);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Creates a private candidate router without changing the live router. "
				+ "The candidate does not run learning.")
				.key(LIVE_KEY, "The live router engine id")
				.key(MAP_KEY, "The proposed router configuration")
				.output(Map.class, "The private candidate engine id");
	}
}
