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
 * Lists the delegations assigned to the logged-in user, newest first.
 *
 * <pre>{@code
 * GetAssignedDelegations();
 * GetAssignedDelegations(status=["PENDING"]);
 * }</pre>
 */
public class GetAssignedDelegationsReactor extends AbstractReactor {

	private static final String STATUS_KEY = "status";

	public GetAssignedDelegationsReactor() {
		this.keysToGet = new String[] { STATUS_KEY };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String status = StringUtils.trimToNull(this.keyValue.get(STATUS_KEY));
		return new NounMetadata(HumanDelegationService.listAssigned(this.insight, status), PixelDataType.VECTOR,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "List delegations assigned to the logged-in user, with each delegation's room, question, context, and status.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(STATUS_KEY)) {
			return "Optional status filter: PENDING, RESPONDED, DECLINED, or CANCELLED.";
		}
		return super.getDescriptionForKey(key);
	}
}
