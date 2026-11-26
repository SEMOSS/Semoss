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

/**
 * What a sandbox can do, so callers ask instead of checking the mode.
 *
 * @param sharesHostFilesystem the worker sees the host's files at the same paths
 * @param liveGrants           a folder granted after the worker started is visible to it
 * @param revoke               {@link ISandbox#revoke(String)} can take a grant back
 * @param needsSync            files must be copied in and written back (the sandbox has its own
 *                             filesystem)
 * @param ownsShellState       {@link ISandbox#exec} tracks the working directory itself, so the caller
 *                             passes it the whole command line including {@code cd}, {@code pwd} and
 *                             {@code reset}
 */
public record Capabilities(boolean sharesHostFilesystem, boolean liveGrants, boolean revoke, boolean needsSync,
		boolean ownsShellState) {
}
