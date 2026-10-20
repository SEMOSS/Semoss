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
package prerna.reactor.agent.run;

import java.util.Locale;

/**
 * Controls how a subagent run reports completion to its parent run.
 *
 * <p>
 * {@link #WAIT}: the parent collects the result with WaitForSubAgent.
 * {@link #POST}: the result is posted to the parent room.
 * {@link #POST_AND_CONTINUE}: the result is posted and one new parent-room run
 * starts.
 */
public enum SubAgentRunCompletionMode {

	WAIT, POST, POST_AND_CONTINUE;

	public static SubAgentRunCompletionMode fromExternalValue(String value) {
		if (value == null || value.trim().isEmpty()) {
			return WAIT;
		}
		return valueOf(value.trim().toUpperCase(Locale.ROOT));
	}

	/**
	 * Read the persisted value without allowing old or unknown data to opt into
	 * asynchronous behavior. Requests written before this field existed therefore
	 * retain the WAIT behavior.
	 */
	public static SubAgentRunCompletionMode fromPersistedValue(Object value) {
		if (value == null) {
			return WAIT;
		}
		try {
			return fromExternalValue(String.valueOf(value));
		} catch (IllegalArgumentException ignored) {
			return WAIT;
		}
	}
}
