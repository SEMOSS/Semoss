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

import java.util.List;

/**
 * Thrown when a permission batch cannot be saved, with every problem found. It
 * is an {@link IllegalArgumentException}, which the endpoints answer with a
 * 400.
 */
public class PermissionBatchException extends IllegalArgumentException {

	private static final long serialVersionUID = 1L;

	private final transient List<PermissionProblem> problems;

	/**
	 * Creates the exception with its message and a copy of the problems found.
	 *
	 * @param message  what to show
	 * @param problems every problem found, the first one first
	 */
	public PermissionBatchException(String message, List<PermissionProblem> problems) {
		super(message);
		this.problems = List.copyOf(problems);
	}

	/**
	 * The problems that kept the batches from being saved, which the caller can
	 * report or turn into its own exception.
	 *
	 * @return every problem found, the first one first
	 */
	public List<PermissionProblem> getProblems() {
		return problems;
	}
}
