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
package prerna.util;

import java.lang.reflect.Field;
import java.util.Properties;

import prerna.engine.api.IEngine;

/**
 * Changes to the real {@link DIHelper} singleton that only tests make: starting
 * it over, putting a stand-in engine in as loaded, and taking registrations out
 * again after a test. Everything else a test needs goes through
 * {@link InternalUtility} like the code under test.
 */
public final class DIHelperTestUtils {

	private DIHelperTestUtils() {
	}

	/**
	 * Throws away the DIHelper singleton and starts a new one that holds the
	 * given RDF_Map settings and nothing else.
	 *
	 * @param coreProp the RDF_Map settings for the new singleton
	 * @throws ReflectiveOperationException if the singleton field cannot be reset
	 */
	public static void reset(Properties coreProp) throws ReflectiveOperationException {
		Field field = DIHelper.class.getDeclaredField("helper");
		field.setAccessible(true);
		field.set(null, null);
		DIHelper.getInstance().setCoreProp(coreProp);
	}

	/**
	 * Puts an engine in DIHelper as loaded, so {@code Utility.getEngine} returns
	 * it without reading an smss.
	 *
	 * @param engineId the engine id
	 * @param engine   the engine to return
	 */
	public static void setLoadedEngine(String engineId, IEngine engine) {
		DIHelper.getInstance().setEngineProperty(engineId, engine);
	}

	/**
	 * Takes the smss path of an engine out of DIHelper.
	 *
	 * @param engineId the engine id
	 */
	public static void removeEngineSmssPath(String engineId) {
		DIHelper.getInstance().removeEngineProperty(engineId + "_" + Constants.STORE);
	}

	/**
	 * Takes a loaded project out of DIHelper.
	 *
	 * @param projectId the project id
	 */
	public static void removeLoadedProject(String projectId) {
		DIHelper.getInstance().removeProjectProperty(projectId);
	}

	/**
	 * Takes the smss path of a project out of DIHelper.
	 *
	 * @param projectId the project id
	 */
	public static void removeProjectSmssPath(String projectId) {
		DIHelper.getInstance().removeProjectProperty(projectId + "_" + Constants.STORE);
	}
}
