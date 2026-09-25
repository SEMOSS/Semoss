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
package prerna.engine.impl.model.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.cluster.util.ClusterUtil;
import prerna.util.Constants;
import prerna.util.Utility;

class MessageInputMediaUnitTests {
	@TempDir
	Path directory;

	@Test
	void readsMediaOnlyFromValidatedRoomFolder() throws Exception {
		Path roomRoot = Files.createDirectory(directory.resolve(Constants.ROOM_FOLDER));
		Path roomFolder = Files.createDirectory(roomRoot.resolve("room-123"));
		Path nestedFolder = Files.createDirectory(roomFolder.resolve("nested"));
		Files.write(nestedFolder.resolve("media.png"), new byte[] { 1, 2, 3 });

		try (MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<ClusterUtil> cluster = mockStatic(ClusterUtil.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(directory.toString());
			MessageInputMedia media = MessageInputMedia.fromFile("nested/media.png", "room-123", null,
					roomFolder.toString());
			assertEquals("AQID", media.getBase64Data());

			assertThrows(IllegalArgumentException.class,
					() -> MessageInputMedia.fromFile("../outside.png", "room-123", null, roomFolder.toString()));
			assertThrows(IllegalArgumentException.class, () -> MessageInputMedia.fromFile(
					directory.resolve("outside.png").toString(), "room-123", null, roomFolder.toString()));
			assertThrows(IllegalArgumentException.class, () -> MessageInputMedia.fromFile("media.png", "room-123",
					null, directory.resolve("outside").toString()));
		}
	}

	@Test
	void rejectsMediaSymlinkOutsideRoomFolder() throws Exception {
		Path roomRoot = Files.createDirectory(directory.resolve(Constants.ROOM_FOLDER));
		Path roomFolder = Files.createDirectory(roomRoot.resolve("room-123"));
		Path outsideFile = Files.write(directory.resolve("outside.png"), new byte[] { 1, 2, 3 });
		Files.createSymbolicLink(roomFolder.resolve("media.png"), outsideFile);

		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(directory.toString());
			assertThrows(IllegalArgumentException.class,
					() -> MessageInputMedia.fromFile("media.png", "room-123", null, roomFolder.toString()));
		}
	}
}
