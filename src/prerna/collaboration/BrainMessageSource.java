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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;

// where the thread read gets message bodies at call time; nothing behind this stores them
public interface BrainMessageSource {

	// points at a brain-mail-v1 style mail.json (RDF_Map, else an environment variable); local testing only,
	// never set in production: mail then comes from the file, not Graph
	String FIXTURE_SETTING = "COLLABORATION_FIXTURE_MAIL";

	// one message in Graph shape (from, subject, body, uniqueBody, receivedDateTime, conversationId), null if gone
	Map<String, Object> fetch(User user, String source, String conversationId, String graphId) throws Exception;

	// a message, null when gone, or why it could not be read
	record Fetched(Map<String, Object> message, Exception error) {
	}

	// several messages of one conversation, in order; a login problem (SemossPixelException) is thrown
	default List<Fetched> fetchAll(User user, String source, String conversationId, List<String> graphIds) {
		List<Fetched> out = new ArrayList<>();
		for (String graphId : graphIds) {
			try {
				out.add(new Fetched(fetch(user, source, conversationId, graphId), null));
			} catch (SemossPixelException e) {
				throw e;
			} catch (Exception e) {
				out.add(new Fetched(null, e));
			}
		}
		return out;
	}

	static BrainMessageSource current() {
		String fixture = fixturePath();
		return fixture == null ? new BrainGraphMessageSource() : BrainFixtureMessageSource.of(fixture);
	}

	// the fixture mail.json, or null to read Graph
	static String fixturePath() {
		String fixture = Utility.getDIHelperProperty(FIXTURE_SETTING);
		if (fixture == null || fixture.isBlank()) {
			fixture = System.getenv(FIXTURE_SETTING);
		}
		return fixture == null || fixture.isBlank() ? null : fixture.trim();
	}
}
