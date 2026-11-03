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
package prerna.reactor.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.impl.model.inferencetracking.GuardrailHitStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Lists the users locked out of an engine after their guardrail hits reached
 * one of its hit limits, ending soonest first, with each user's name and email
 * and when the lock started and ends. Requires edit access to the engine.
 */
public class GetEngineGuardrailLocksReactor extends AbstractReactor {

	public GetEngineGuardrailLocksReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have edit access to it");
		}

		List<GuardrailHitStore.Lock> locks;
		try {
			locks = GuardrailHitStore.activeLocks(engineId, Utility.getCurrentSqlTimestampUTC());
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the guardrail locks of engine " + engineId, e);
		}
		List<Pair<String, String>> lockedUsers = new ArrayList<>();
		for (GuardrailHitStore.Lock lock : locks) {
			lockedUsers.add(Pair.with(lock.userId(), lock.userType()));
		}
		Map<Pair<String, String>, String[]> people = SecurityUserUtils.getNamesAndEmails(lockedUsers);

		List<Map<String, Object>> rows = new ArrayList<>();
		for (GuardrailHitStore.Lock lock : locks) {
			String[] person = people.get(Pair.with(lock.userId(), lock.userType()));
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("userId", lock.userId());
			row.put("userType", lock.userType());
			row.put("name", person == null ? null : person[0]);
			row.put("email", person == null ? null : person[1]);
			row.put("limitId", lock.limitId());
			row.put("lockedAt", Utility.getSqlTimestampToString(lock.lockedAt()));
			row.put("lockedUntil", Utility.getSqlTimestampToString(lock.lockedUntil()));
			rows.add(row);
		}
		return new NounMetadata(rows, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Lists the users locked out of an engine after their guardrail hits reached one of its hit limits, ending soonest first: each user's id, login type, name and email, the limit, and when the lock started and ends, in UTC. Requires edit access to the engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the engine";
		}
		return super.getDescriptionForKey(key);
	}
}
