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
package prerna.reactor.function;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ExecuteGuardrailEngineReactor extends ExecuteReactorFunctionEngineReactor {

	/*
	 * Just a convenience method
	 * Works the same as the reactor function engine
	 * Since guardrail engine is a reactor function engine as well
	 */

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(getUnableToAccessError(engineId));
		}

		// remove the engine id from the reactor noun store so it is not passed to the
		// guardrail engine
		if (this.store.getGenRowStruct(ReactorKeysEnum.ENGINE.getKey()) != null) {
			this.store.removeNoun(ReactorKeysEnum.ENGINE.getKey());
		} else {
			for (int i = 0; i < this.curRow.size(); i++) {
				if (engineId.equals(this.curRow.get(i))) {
					this.curRow.remove(i);
					break;
				}
			}
		}

		if (this.insight != null) {
			GenRowStruct insightGrs = this.store.makeGenRowStruct(Constants.INSIGHT);
			insightGrs.add(NounMetadata.predictNounMetadata(this.insight));
		}

		IGuardrailReactorFunctionEngine guardrailEngine = Utility.getGuardrailEngine(engineId);
		return guardrailEngine.execute(getNounStore(), this.curRow);
	}
	
	@Override
	String getUnableToAccessError(String engineId) {
		return "Guardrail Engine " + engineId + " does not exist or user does not have access to this guardrail";
	}
}
