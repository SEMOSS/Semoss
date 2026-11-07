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

import prerna.auth.User;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * The end of a served model's container log: vLLM's ({@code kserve-container},
 * the default) or the weight download's ({@code storage-initializer}).
 * Terminal colors are removed, and so are the /health and /metrics access
 * lines unless includeAccessLogs is true.
 *
 * <pre>
 * GetServedModelLogs(servedModel=["olmo-2-1b-instruct"]);
 * GetServedModelLogs(servedModel=["olmo-2-1b-instruct"], container=["storage-initializer"]);
 * GetServedModelLogs(servedModel=["olmo-2-1b-instruct"], tailLines=[50], previous=[true]);
 * </pre>
 */
public class GetServedModelLogsReactor extends AbstractServedModelReactor {

	static final String CONTAINER = "container";
	static final String TAIL_LINES = "tailLines";
	static final String PREVIOUS = "previous";
	static final String INCLUDE_ACCESS_LOGS = "includeAccessLogs";

	public GetServedModelLogsReactor() {
		this.keysToGet = new String[] { SERVED_MODEL, CONTAINER, TAIL_LINES, PREVIOUS, INCLUDE_ACCESS_LOGS };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		String container = this.keyValue.get(CONTAINER);
		if (container == null || container.isBlank()) {
			container = ServedModelService.MAIN_CONTAINER;
		}
		return result(service.logs(getServedModel(), container.strip(), tailLines(),
				Boolean.parseBoolean(this.keyValue.get(PREVIOUS)),
				Boolean.parseBoolean(this.keyValue.get(INCLUDE_ACCESS_LOGS))).toMap());
	}

	private Integer tailLines() {
		String value = this.keyValue.get(TAIL_LINES);
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			// pixel numbers arrive as doubles (50 as 50.0)
			return (int) Double.parseDouble(value.strip());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(TAIL_LINES + " must be a number");
		}
	}

	@Override
	public String getReactorDescription() {
		return "Gets the end of a served model's vLLM or weight-download log. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
		case CONTAINER -> "kserve-container (vLLM, default) or storage-initializer (the weight download)";
		case TAIL_LINES -> "Lines to return, 1 to " + ServedModelService.MAX_LOG_LINES + "; default "
				+ ServedModelService.DEFAULT_LOG_LINES;
		case PREVIOUS -> "true to read the container's previous instance, after a crash";
		case INCLUDE_ACCESS_LOGS -> "true to keep the /health and /metrics access lines";
		default -> super.getDescriptionForKey(key);
		};
	}
}
