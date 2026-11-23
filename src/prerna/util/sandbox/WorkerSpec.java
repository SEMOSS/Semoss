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
package prerna.util.sandbox;

import prerna.auth.User;

/**
 * What a worker needs to start. Fields a kind does not use are ignored.
 *
 * @param kind            which worker
 * @param port            the port to bind, already allocated
 * @param serverDirectory the worker's scratch directory; a jail may replace it with its own
 * @param venvPath        the python executable to use, or null for the base python
 * @param timeout         idle timeout in minutes, "-1" for none
 * @param loggerLevel     log level for the worker
 * @param engineOwned     true for a worker an engine owns, which a jail leaves unscoped
 * @param user            the user the worker belongs to, or null
 */
public record WorkerSpec(WorkerKind kind, int port, String serverDirectory, String venvPath, String timeout,
		String loggerLevel, boolean engineOwned, User user) {
}
