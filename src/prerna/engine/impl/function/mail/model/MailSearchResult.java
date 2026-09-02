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
package prerna.engine.impl.function.mail.model;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What a mailbox search found.
 *
 * <p>
 * Held as a result rather than as the answer itself so that only
 * {@link #toMap()} knows the shape callers have always seen. Every mailbox
 * builds one of these the same way, and the map is made once, at the edge.
 *
 * @param folder     the folder that was read
 * @param messages   the messages found, newest first
 * @param unreadable how many messages were skipped because they could not be
 *                   read, which is reported only when there were any
 */
public record MailSearchResult(String folder, List<Map<String, Object>> messages, int unreadable) {

	public MailSearchResult {
		messages = List.copyOf(messages);
	}

	/**
	 * @return the search in the shape a caller of the engine receives it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("folder", this.folder);
		output.put("count", this.messages.size());
		output.put("messages", this.messages);
		if (this.unreadable > 0) {
			output.put("unreadable", this.unreadable);
		}
		return output;
	}
}
