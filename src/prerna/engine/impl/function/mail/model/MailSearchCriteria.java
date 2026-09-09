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

import java.time.Instant;

/**
 * What a caller asked a mailbox search to match on.
 *
 * <p>
 * Kept separate from the rest of a {@link MailSearchRequest} because these four
 * are the ones a mailbox has to be asked about, where the rest describe what to
 * do with what comes back. The distinction matters to Graph in particular,
 * which cannot combine a text search with a filter and so applies some of these
 * on the server and the rest in memory.
 *
 * @param from       text the sender has to contain, or null
 * @param subject    text the subject has to contain, or null
 * @param since      the oldest message to return, or null for no cutoff
 * @param unreadOnly whether to return only messages nobody has opened
 */
public record MailSearchCriteria(String from, String subject, Instant since, boolean unreadOnly) {

	/**
	 * @return true when nothing was asked for, so every message in the folder
	 *         matches
	 */
	public boolean isEmpty() {
		return this.from == null && this.subject == null && this.since == null && !this.unreadOnly;
	}
}
