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
package prerna.reactor.model.serving;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelState;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the models KServe serves in the configured namespace, defined ones
 * included: a stopped InferenceService stays defined.
 *
 * <pre>
 * ListServedModels();                    // all
 * ListServedModels(state=["active"]);    // starting, waiting for the GPU, ready, failed or stopping
 * ListServedModels(state=["ready"]);
 * </pre>
 */
public class ListServedModelsReactor extends AbstractServedModelReactor {

	static final String STATE = "state";

	public ListServedModelsReactor() {
		this.keysToGet = new String[] { STATE };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String filter = this.keyValue.get(STATE);
		filter = filter == null || filter.isBlank() ? "all" : filter.strip().toLowerCase(Locale.ROOT);
		if (!List.of("all", "active", "ready").contains(filter)) {
			throw new IllegalArgumentException("state must be all, active or ready");
		}
		List<Map<String, Object>> models = new ArrayList<>();
		for (ServedModelStatus status : service.list()) {
			boolean keep = switch (filter) {
			case "active" -> status.getState().isActive();
			case "ready" -> status.getState() == ServedModelState.READY;
			default -> true;
			};
			if (keep) {
				models.add(status.toSummaryMap());
			}
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("namespace", service.getNamespace());
		result.put("servedModels", models);
		return result(result);
	}

	@Override
	public String getReactorDescription() {
		return "Lists the models served by KServe (InferenceServices), stopped ones included, with their state, "
				+ "phase and progress. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (STATE.equals(key)) {
			return "Optional filter: all (default), active (anything not stopped) or ready";
		}
		return super.getDescriptionForKey(key);
	}
}
