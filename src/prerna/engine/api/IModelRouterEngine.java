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

import java.io.IOException;

import prerna.logging.IgnoreEngineLogging;

/**
 * A model engine that delegates each ask to one of several backing model
 * engines based on a routing configuration it can reload at runtime.
 *
 * <p>These methods are declared on an interface rather than only on the
 * implementation because Utility.getModel returns a dynamic proxy over the
 * engine's interfaces (see EngineProxyFactory) - the concrete engine is
 * unreachable through it, so a cast to the implementation class fails. Callers
 * that need the routing config work against this interface and check
 * {@code instanceof IModelRouterEngine} instead of the implementation class.
 *
 * <p>All methods are marked {@link IgnoreEngineLogging}: they are admin-time
 * configuration operations, not model calls, so they should neither produce
 * engine audit rows nor be run through the guardrail pipelines.
 */
public interface IModelRouterEngine extends IModelEngine {

	/**
	 * Raw contents of the routing config file, for the settings UI.
	 *
	 * @return the config file contents
	 * @throws IOException if the config file is missing or unreadable
	 */
	@IgnoreEngineLogging
	String readConfigJson() throws IOException;

	/**
	 * Validates the given JSON, persists it to the config file, and applies it to
	 * the live instance. Nothing is written when validation fails.
	 *
	 * @param json the new routing config
	 * @throws IOException if the config file cannot be written
	 */
	@IgnoreEngineLogging
	void updateConfig(String json) throws IOException;

	/**
	 * Re-reads and applies the config file on the live instance, picking up an
	 * edit made outside of {@link #updateConfig(String)}.
	 *
	 * @throws IOException if the config file is missing or unreadable
	 */
	@IgnoreEngineLogging
	void reloadConfig() throws IOException;

}
