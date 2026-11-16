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
package prerna.engine.impl.pipeline;

/**
 * The app making the engine call running on this thread, when the engine gives
 * that app a permission. The engine proxy sets it when the app's custom reactors
 * call the engine, whether the call went through on the app's permission or on
 * the person's own, and the engine's own code reads it while it answers the
 * call: a model counts the call against that app's App Limit for a person whose
 * only way to the model is the app.
 */
public final class EngineCallAppPermission {

	private static final ThreadLocal<String> APP_MAKING_THE_CALL = new ThreadLocal<>();

	private EngineCallAppPermission() {
	}

	/**
	 * The app whose custom reactors made the call being answered on this thread,
	 * when the engine gives that app a permission.
	 *
	 * @return the app's project id, or null for a call made by the platform, by
	 *         another engine, or by an app the engine gives no permission
	 */
	public static String appMakingTheCall() {
		return APP_MAKING_THE_CALL.get();
	}

	/**
	 * Marks the start of a call the proxy passes on to its engine. A call made
	 * inside another call gets its own value, so an engine called by the engine an
	 * app called does not see the app.
	 *
	 * @param appId the app making the call, which the engine gives a permission,
	 *              or null
	 * @return the app of the call this one runs inside, for
	 *         {@link #restore(String)}
	 */
	static String begin(String appId) {
		String previousAppId = APP_MAKING_THE_CALL.get();
		if (appId == null) {
			if (previousAppId != null) {
				APP_MAKING_THE_CALL.remove();
			}
		} else {
			APP_MAKING_THE_CALL.set(appId);
		}
		return previousAppId;
	}

	/**
	 * Marks the end of a call, putting back the app of the call it ran inside.
	 *
	 * @param previousAppId what {@link #begin(String)} returned
	 */
	static void restore(String previousAppId) {
		if (previousAppId == null) {
			APP_MAKING_THE_CALL.remove();
		} else {
			APP_MAKING_THE_CALL.set(previousAppId);
		}
	}

}
