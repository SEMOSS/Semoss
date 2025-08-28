/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.function;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExecuteReactorFunctionEngineReactor extends AbstractReactor {

	public ExecuteReactorFunctionEngineReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[]{1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(getUnableToAccessError(engineId));
		}

		// remove the engine id from the reactor noun store
		// so it is not passed to the engine
		// this is so it doesn't mess with the keyValue prediction
		// within an engine execute
		if (this.store.getNoun(ReactorKeysEnum.ENGINE.getKey()) != null) {
			this.store.removeNoun(ReactorKeysEnum.ENGINE.getKey());
		} else {
			for (int i = 0; i < this.curRow.size(); i++) {
				if (engineId.equals(this.curRow.get(i))) {
					this.curRow.remove(i);
					break;
				}
			}
		}

		IReactorFunctionEngine reactorFunctionEngine = Utility.getReactorEngine(engineId);
		return reactorFunctionEngine.execute(getNounStore(), this.curRow);
	}

	String getUnableToAccessError(String engineId) {
		return "Reactor Function Engine " + engineId + " does not exist or user does not have access to this function";
	}
}
