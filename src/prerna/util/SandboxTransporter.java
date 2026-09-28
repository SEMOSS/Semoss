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

/**
 * Moves host paths into an external sandbox.
 *
 * <p>
 * The OPENSHELL counterpart of {@link SandboxInjector}. Both are the per-mode collaborator that
 * {@link SymlinkHelper} hands a path to when it would otherwise have symlinked it:
 *
 * <table>
 * <caption>mode collaborators</caption>
 * <tr><th>Mode</th><th>Collaborator</th><th>What it does with the path</th></tr>
 * <tr><td>NAMESPACE</td><td>{@link SandboxInjector}</td><td>adds it to the namespace jail's
 * allowed set over the control socket</td></tr>
 * <tr><td>OPENSHELL</td><td>{@link SandboxTransporter}</td><td>copies the tree into the
 * sandbox</td></tr>
 * </table>
 *
 * <p>
 * The distinction is worth stating because it is not an implementation detail: a namespace jail runs
 * on this host, so binding the real path is enough. A sandbox has its own filesystem, so the bytes
 * have to be transferred. <b>Making the path visible is not making it present.</b> A worker that calls
 * {@code sys.path.insert} on an asset path, or opens an asset by absolute path, fails with
 * {@code FileNotFoundError} when only the path exists -- observed with a project MCP driver, where
 * {@code PyTranslator} passed the asset paths correctly and the import still failed because the file
 * was never in the sandbox.
 */
public interface SandboxTransporter {

	/**
	 * Make {@code absPath} (a file or directory tree on this host) available inside the sandbox at the
	 * same absolute path.
	 *
	 * <p>
	 * Implementations must be idempotent: the same path is requested repeatedly (every insight,
	 * project open and MCP call re-symlinks), and re-transferring a tree on each call would be a
	 * per-request cost with no benefit.
	 *
	 * @param absPath host path, already normalised
	 * @return true if the path is present in the sandbox after the call
	 */
	boolean transport(String absPath);

	/**
	 * Best-effort teardown.
	 *
	 * @return true if the collaborator released cleanly
	 */
	boolean close();
}
