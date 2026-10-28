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
import prerna.engine.impl.model.serving.ServingGpuStatus;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * What the GPUs are doing: each GPU's utilization, power, temperature and SM
 * clock from the GPU Operator's DCGM exporter (through Prometheus), the pods
 * using it, its model, driver and CUDA version, and each node's memory, with a
 * history of 60 points over the window for sparklines.
 *
 * <pre>
 * GetServingGpuStatus();
 * GetServingGpuStatus(window=["24h"]);
 * </pre>
 */
public class GetServingGpuStatusReactor extends AbstractServedModelReactor {

	static final String WINDOW = "window";

	public GetServingGpuStatusReactor() {
		this.keysToGet = new String[] { WINDOW };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	protected NounMetadata run(User user, ServedModelService service) {
		return result(service.gpuStatus(ServingGpuStatus.Window.parse(this.keyValue.get(WINDOW))).toMap());
	}

	@Override
	public String getReactorDescription() {
		return "What the GPUs serving models are doing: utilization, power, temperature, clock, the pods using them, "
				+ "and the nodes' memory, with a recent history. Admins only";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (WINDOW.equals(key)) {
			return "How far back the history goes: 15m, 1h (default), 6h or 24h";
		}
		return super.getDescriptionForKey(key);
	}
}
