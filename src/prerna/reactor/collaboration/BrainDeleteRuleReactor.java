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
package prerna.reactor.collaboration;

import prerna.auth.User;
import prerna.collaboration.BrainRuleUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainDeleteRule(ruleId=["..."]);
public class BrainDeleteRuleReactor extends AbstractCollaborationReactor {

	private static final String RULE_ID = "ruleId";

	public BrainDeleteRuleReactor() {
		this.keysToGet = new String[] { RULE_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String ruleId = getString(RULE_ID);
		if (ruleId == null) {
			throw new IllegalArgumentException("Must pass a ruleId");
		}
		return mapResult(BrainRuleUtils.deleteRule(user, ruleId));
	}

	@Override
	public String getReactorDescription() {
		return "Turns off a Brain rule; people it excluded on threads are included again; owner only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RULE_ID.equals(key)) {
			return "Rule id";
		}
		return super.getDescriptionForKey(key);
	}
}
