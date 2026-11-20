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
package prerna.reactor.agent;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.HumanDelegationService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Declines a delegation assigned to the logged-in user and closes it.
 *
 * <pre>{@code
 * DeclineDelegation(actionId=["<actionId>"], reason=["Not my area"]);
 * }</pre>
 */
public class DeclineDelegationReactor extends AbstractReactor {

	private static final String ACTION_ID_KEY = "actionId";
	private static final String REASON_KEY = "reason";

	public DeclineDelegationReactor() {
		this.keysToGet = new String[] { ACTION_ID_KEY, REASON_KEY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String actionId = StringUtils.trimToNull(this.keyValue.get(ACTION_ID_KEY));
		if (actionId == null) {
			throw new IllegalArgumentException("actionId is required");
		}
		String reason = StringUtils.trimToNull(this.keyValue.get(REASON_KEY));
		return new NounMetadata(HumanDelegationService.decline(this.insight, actionId, reason), PixelDataType.MAP,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Decline a delegation assigned to the logged-in user. The optional reason is sent to the requester.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION_ID_KEY)) {
			return "The delegation's action ID, as listed by GetAssignedDelegations.";
		}
		if (key.equals(REASON_KEY)) {
			return "Optional reason sent back to the requester.";
		}
		return super.getDescriptionForKey(key);
	}
}
