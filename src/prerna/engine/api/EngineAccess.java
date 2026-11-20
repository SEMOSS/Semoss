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
package prerna.engine.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import prerna.auth.AccessPermissionEnum;

/**
 * The permission a call needs on an engine or project to run one of its
 * methods. Every method of the engine and project interfaces carries one, and
 * the engine proxy checks it on every call before the call reaches the engine,
 * whoever makes it: the platform for a user, a project's custom code, or
 * another engine's own code.
 *
 * <p>
 * {@link #value()} is the level every call needs. {@link #projectCode()} is a
 * higher level for a call a project's custom code makes itself, on the methods
 * that hand back more than the platform's own callers pass on: a raw
 * connection, or the engine's settings. A call meets its level through the
 * signed in user's permission, or through the permission the engine gives the
 * app or engine making the call.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EngineAccess {

	/**
	 * The permission every call to the method needs, whoever makes it.
	 *
	 * @return the level
	 */
	AccessPermissionEnum value();

	/**
	 * The permission needed when a project's custom code calls the method itself.
	 * Empty means the same level as {@link #value()}; otherwise one level, above
	 * it.
	 *
	 * @return no level, or the one level for project code
	 */
	AccessPermissionEnum[] projectCode() default {};

}
