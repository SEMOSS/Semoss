package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.playground.PlaygroundUtils;

class CollaborationUtilsUnitTests {

	private static Room room(String projectId, Map<String, Object> options) {
		Room room = new Room();
		room.setProjectId(projectId);
		room.setOptionsMap(new HashMap<>(options));
		return room;
	}

	private static Room collaboration(Map<String, Object> options) {
		return room(CollaborationUtils.COLLABORATION_PROJECT_ID, options);
	}

	@Test
	void aHomeChatIsTheOwnersAssistantWithNoThread() {
		Room home = collaboration(Map.of("modelId", "m-1"));
		assertTrue(CollaborationUtils.isAssistantRoom(home));
		assertNull(CollaborationUtils.threadIdOf(home));
	}

	@Test
	void aRoomOpenedFromAThreadNamesItsThread() {
		Room opened = collaboration(Map.of(CollaborationUtils.ROOM_OPTION_SOURCE,
				Map.of("version", 1, "threadId", "th-1", "channel", "email")));
		assertTrue(CollaborationUtils.isAssistantRoom(opened));
		assertEquals("th-1", CollaborationUtils.threadIdOf(opened));
	}

	@Test
	void anOlderWorkThreadRoomStillNamesItsThread() {
		Room older = collaboration(Map.of(CollaborationUtils.ROOM_OPTION_WORK_THREAD,
				Map.of("threadId", "session:9f0c", "contextRevision", "")));
		assertTrue(CollaborationUtils.isAssistantRoom(older));
		assertEquals("session:9f0c", CollaborationUtils.threadIdOf(older));
	}

	@Test
	void aBlankThreadIdIsNoThread() {
		assertNull(CollaborationUtils.threadIdOf(collaboration(Map.of(CollaborationUtils.ROOM_OPTION_SOURCE,
				Map.of("threadId", " ")))));
	}

	@Test
	void aDelegationRoomIsNotAnAssistantRoom() {
		Room delegation = collaboration(Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "a-1",
				CollaborationUtils.ROOM_OPTION_SOURCE, Map.of("threadId", "th-1")));
		assertFalse(CollaborationUtils.isAssistantRoom(delegation));
		assertNull(CollaborationUtils.threadIdOf(delegation));
	}

	@Test
	void aPlaygroundRoomIsNotAnAssistantRoom() {
		Room playground = room(PlaygroundUtils.PLAYGROUND_PROJECT_ID,
				Map.of(CollaborationUtils.ROOM_OPTION_SOURCE, Map.of("threadId", "th-1")));
		assertFalse(CollaborationUtils.isAssistantRoom(playground));
		assertNull(CollaborationUtils.threadIdOf(playground));
		assertFalse(CollaborationUtils.isAssistantRoom(null));
	}
}
