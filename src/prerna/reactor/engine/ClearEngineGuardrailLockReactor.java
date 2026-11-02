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

import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.GuardrailHitStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.GuardrailAudit;
import prerna.util.Utility;

/**
 * Ends a user's guardrail lock on an engine early, so they can use it again
 * before the lock runs out. Requires edit access to the engine, and is recorded
 * in the audit trail.
 */
public class ClearEngineGuardrailLockReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";
	private static final String USER_TYPE_KEY = "userType";

	public ClearEngineGuardrailLockReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), USER_ID_KEY, USER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		String userId = StringUtils.trimToNull(this.keyValue.get(USER_ID_KEY));
		String userType = StringUtils.trimToNull(this.keyValue.get(USER_TYPE_KEY));
		if (userId == null || userType == null) {
			throw new IllegalArgumentException("The locked user's id and login type are required");
		}
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have edit access to it");
		}

		Pair<String, String> lockedUser = Pair.with(userId, userType);
		boolean cleared;
		try {
			cleared = GuardrailHitStore.clearLock(engineId, lockedUser, User.getPrimaryUserIdAndTypePair(user),
					Utility.getCurrentSqlTimestampUTC());
		} catch (Exception e) {
			throw new IllegalStateException("Unable to clear the guardrail lock on engine " + engineId, e);
		}
		NounMetadata noun = new NounMetadata(cleared, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
		if (!cleared) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage("The user is not locked out of this engine"));
			return noun;
		}
		GuardrailAudit.recordLockClear(user, engineId, SecurityEngineUtils.getEngineAliasForId(engineId),
				(IEngine.CATALOG_TYPE) SecurityEngineUtils.getEngineTypeAndSubtype(engineId)[0], lockedUser);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("The user can use this engine again"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Ends a user's guardrail lock on an engine early, so they can use it again before the lock runs out. Requires edit access to the engine, and is recorded in the audit trail. Returns whether a lock was in force.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the engine";
		} else if (USER_ID_KEY.equals(key)) {
			return "The locked user's id";
		} else if (USER_TYPE_KEY.equals(key)) {
			return "The locked user's login type, such as NATIVE";
		}
		return super.getDescriptionForKey(key);
	}
}
