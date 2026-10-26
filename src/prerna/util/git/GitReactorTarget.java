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
package prerna.util.git;

import prerna.auth.User;
import prerna.sablecc2.om.PixelOperationType;

/**
 * Everything that differs between the engine and the project flavor of a git
 * working-tree reactor: which pixel key carries the id, how the caller's
 * permission is checked, how the id resolves to a version folder, and how the
 * response is typed. Implementations are stateless singletons supplied to
 * {@link AbstractGitWorktreeReactor} through its constructor.
 */
public interface GitReactorTarget {

	/**
	 * The pixel key the target id is passed under, e.g.
	 * {@code ReactorKeysEnum.ENGINE.getKey()}.
	 */
	String getIdKey();

	/**
	 * Lower case noun used in error messages and key descriptions, e.g. "engine".
	 */
	String getLabel();

	/** The label with its indefinite article, e.g. "an engine". */
	String getLabelWithArticle();

	/** The label capitalized for the start of a sentence, e.g. "Engine". */
	default String getCapitalizedLabel() {
		String label = getLabel();
		return Character.toUpperCase(label.charAt(0)) + label.substring(1);
	}

	/** The operation type every reactor for this target returns. */
	PixelOperationType getOpType();

	boolean userCanView(User user, String targetId);

	boolean userCanEdit(User user, String targetId);

	/**
	 * Loads the target and locates the version folder holding its git working tree.
	 * Called only after the permission check has passed.
	 */
	GitTargetHandle resolve(String targetId);
}
