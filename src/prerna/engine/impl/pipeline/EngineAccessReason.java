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
 * Why the access check let a call to an engine or a project through, or that it
 * refused the call. Each engine call the Activity Log records carries one, with
 * its details ({@link EngineAccessDecision}).
 */
public enum EngineAccessReason {

	/** The person's own grant */
	DIRECT,
	/**
	 * A grant to a team the person is in: a custom team or a login provider's group
	 */
	TEAM,
	/** The engine or project is public, and the call only reads it */
	PUBLIC,
	/**
	 * The engine or project gave an app, an agent or another engine a permission,
	 * which the app's custom code, the agent's run or the engine's own code used
	 */
	DELEGATED,
	/** An app's custom code on its own project */
	OWN_PROJECT,
	/** A project read through one of its insights the person can open */
	INSIGHT,
	/** A discoverable engine or project read through the platform */
	DISCOVERABLE,
	/** An admin, while an admin reactor or endpoint is acting */
	ADMIN,
	/**
	 * The platform's own work: loading an engine, a proxy handed out for its own
	 * use, a platform operation, an engine calling itself, and the like
	 */
	PLATFORM,
	/** Nothing let the call through */
	REFUSED

}
