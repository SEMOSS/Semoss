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
package prerna.io.connector.mail;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;

/**
 * One place mail can be filed: an Outlook folder, or a Gmail label.
 *
 * @param id          what the mail reactors take to read or move into it
 * @param name        its name as a person knows it
 * @param kind        one of {@link #FOLDER}, {@link #LABEL} or {@link #SYSTEM}
 * @param totalCount  how many messages it holds, when the provider says
 * @param unreadCount how many of them nobody has opened, when the provider says
 */
public record MailFolder(String id, String name, String kind, Long totalCount, Long unreadCount) {

	/** An Outlook folder. */
	public static final String FOLDER = "folder";

	/** A label somebody made in Gmail. */
	public static final String LABEL = "label";

	/** A label Gmail itself keeps, such as the inbox or the trash. */
	public static final String SYSTEM = "system";

	/**
	 * @return the folder as a reactor answers with it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", this.id);
		ConnectorOutput.putIfPresent(output, "name", this.name);
		output.put("kind", this.kind);
		ConnectorOutput.putIfPresent(output, "totalCount", this.totalCount);
		ConnectorOutput.putIfPresent(output, "unreadCount", this.unreadCount);
		return output;
	}
}
