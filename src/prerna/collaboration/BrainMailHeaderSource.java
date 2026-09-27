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
package prerna.collaboration;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import prerna.auth.User;

// onboarding and Refresh: message headers only (no body), in Graph shape, for the import and the mailbox overview
public interface BrainMailHeaderSource {

	String INBOX = "inbox";
	String SENT = "sentitems";
	List<String> FOLDERS = List.of(INBOX, SENT);

	// the signed-in mailbox: id, displayName, mail, userPrincipalName
	Map<String, Object> me(User user) throws Exception;

	// the owner's manager (displayName, mail), or null
	Map<String, Object> manager(User user) throws Exception;

	// the owner's other addresses (aliases), lower case; empty when unknown
	default List<String> aliases(User user) {
		return List.of();
	}

	// headers received at or after since, newest first, at most max
	List<Map<String, Object>> list(User user, String folder, Instant since, int max) throws Exception;

	static BrainMailHeaderSource current() {
		String fixture = System.getenv(BrainMessageSource.FIXTURE_ENV);
		return fixture == null || fixture.isBlank() ? new BrainGraphHeaderSource() : BrainFixtureHeaderSource.of(fixture);
	}
}
