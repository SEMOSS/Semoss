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
package prerna.auth.access;

/**
 * An app or an engine given a permission on another engine, by its id and its
 * catalog type: {@link PermissionSubject#PROJECT_TYPE} for an app, whose custom
 * reactors use the permission, and an engine's catalog type, such as
 * {@code MODEL} or {@code GUARDRAIL}, for an engine whose own code uses it.
 *
 * @param engineId   the app's or engine's id
 * @param engineType its catalog type
 */
public record EngineGrantee(String engineId, String engineType) {

	/**
	 * The grantee for an app.
	 *
	 * @param projectId the app's project id
	 * @return the app
	 */
	public static EngineGrantee ofApp(String projectId) {
		return new EngineGrantee(projectId, PermissionSubject.PROJECT_TYPE);
	}

	/**
	 * Whether the grantee is an app.
	 *
	 * @return whether its catalog type is {@link PermissionSubject#PROJECT_TYPE}
	 */
	public boolean isApp() {
		return PermissionSubject.PROJECT_TYPE.equals(engineType);
	}

	/**
	 * The key of the grantee's limit on a model.
	 *
	 * @param modelId the model
	 * @return the App Limit or the Engine Limit on the model
	 */
	public ModelUsageLimitKey limitKeyOn(String modelId) {
		return ModelUsageLimitKey.grantee(modelId, engineId, engineType);
	}
}
