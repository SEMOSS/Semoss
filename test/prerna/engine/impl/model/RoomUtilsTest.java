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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.redis.RedisConnectionConfig;
import prerna.util.Constants;
import prerna.util.Utility;

class RoomUtilsTest {

	@Test
	void cachedRoomRefreshHoldsMutationLock() {
		String roomId = "room-with-in-flight-stream";
		Timestamp now = Timestamp.from(Instant.now());
		Room room = new Room(roomId, "user", "Room", null, null, null, true, now, now, "[]", false, null, null,
				null);

		AccessToken token = mock(AccessToken.class);
		when(token.getId()).thenReturn("user");
		User user = mock(User.class);
		when(user.getPrimaryLoginToken()).thenReturn(token);
		when(user.getRoomHash()).thenReturn(Map.of(roomId, room));
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);

		RoomMessageStore.RoomMutationLock mutationLock = mock(RoomMessageStore.RoomMutationLock.class);
		AtomicBoolean lockAcquired = new AtomicBoolean();
		AtomicBoolean lockClosed = new AtomicBoolean();

		try (MockedStatic<RedisConnectionConfig> redisConfig = mockStatic(RedisConnectionConfig.class);
				MockedStatic<RoomMessageStore> messageStore = mockStatic(RoomMessageStore.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			redisConfig.when(RedisConnectionConfig::isRedisEnabled).thenReturn(true);
			messageStore.when(() -> RoomMessageStore.acquireMutationLock(room)).thenAnswer(invocation -> {
				lockAcquired.set(true);
				return mutationLock;
			});
			messageStore.when(() -> RoomMessageStore.refreshFromLatestProjection(room, "user")).thenAnswer(invocation -> {
				assertTrue(lockAcquired.get(), "cached projection was refreshed before acquiring the mutation lock");
				assertFalse(lockClosed.get(), "cached projection was refreshed after releasing the mutation lock");
				assertSame(insight, room.getInsight(), "request context was not attached under the mutation lock");
				return null;
			});
			org.mockito.Mockito.doAnswer(invocation -> {
				lockClosed.set(true);
				return null;
			}).when(mutationLock).close();
			utility.when(() -> Utility.getDIHelperProperty(Constants.CHROOT_ENABLE)).thenReturn("false");

			assertSame(room, RoomUtils.getOrLoadRoom(roomId, insight));
			assertTrue(lockClosed.get(), "cached room mutation lock was not released");
		}
	}
}
