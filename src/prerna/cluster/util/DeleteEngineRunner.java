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
package prerna.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;

public class DeleteEngineRunner implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineRunner.class);

	private final String ENGINE_ID;
	private final IEngine.CATALOG_TYPE ENGINE_TYPE;

	public DeleteEngineRunner(String engineId, IEngine.CATALOG_TYPE engineType) {
		this.ENGINE_ID = engineId;
		this.ENGINE_TYPE = engineType;
	}

	@Override
	public void run() {
		try {
			ClusterUtil.deleteEngine(ENGINE_ID, ENGINE_TYPE);
		} catch (Exception e) {
			classLogger.error("An error occurred trying to delete the engine {} of type {} from cloud storage",
					ENGINE_ID, ENGINE_TYPE, e);
		}
	}

}
