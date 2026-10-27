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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import prerna.auth.User;

// where topic links read Microsoft 365 drive items; items are MicrosoftOneDriveItemMapper.toDriveItem maps
public interface BrainDriveSource {

	// the item is gone (404) or the owner cannot read it (403)
	final class Unavailable extends RuntimeException {
		private static final long serialVersionUID = 1L;
		private final boolean forbidden;

		public Unavailable(boolean forbidden, String message) {
			super(message);
			this.forbidden = forbidden;
		}

		public boolean forbidden() {
			return forbidden;
		}
	}

	Map<String, Object> item(User user, String driveId, String itemId) throws Exception;

	// a sharing link or a file's web url
	Map<String, Object> resolve(User user, String shareUrl) throws Exception;

	// driveId null is the owner's own OneDrive, folderId null its root
	List<Map<String, Object>> children(User user, String driveId, String folderId, int limit) throws Exception;

	List<Map<String, Object>> shared(User user, int limit) throws Exception;

	List<Map<String, Object>> search(User user, String query, int limit) throws Exception;

	// the owner's teams, channels and followed sites as id, name and webUrl
	List<Map<String, Object>> teams(User user) throws Exception;

	List<Map<String, Object>> channels(User user, String teamId) throws Exception;

	List<Map<String, Object>> sites(User user) throws Exception;

	// a site's document libraries; id is the drive id
	List<Map<String, Object>> libraries(User user, String siteId) throws Exception;

	// a channel's files folder as driveId and itemId
	Map<String, Object> channelFolder(User user, String teamId, String channelId) throws Exception;

	// writes the file into dir under its drive name and returns it
	Path download(User user, String driveId, String itemId, Path dir) throws Exception;

	static BrainDriveSource current() {
		String fixture = BrainMessageSource.fixturePath();
		return fixture == null ? new BrainGraphDriveSource() : BrainFixtureDriveSource.of(fixture);
	}
}
