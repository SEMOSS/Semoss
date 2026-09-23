package prerna.collaboration;

import java.util.List;

import prerna.engine.impl.model.Room;
import prerna.playground.PlaygroundUtils;

/** Collaboration rooms are playground-style rooms under their own system project id. */
public final class CollaborationUtils {

	public static final String COLLABORATION_PROJECT_ID = "SYSTEM__COLLABORATION";
	public static final String MODE_COLLABORATION = "collaboration";
	// Set on an assignee's room; links it to the request it answers.
	public static final String ROOM_OPTION_DELEGATION_ACTION_ID = "delegation_action_id";
	// Only the server sets these; client option writes cannot add, change, or drop them.
	public static final List<String> SERVER_OWNED_ROOM_OPTIONS = List.of(ROOM_OPTION_DELEGATION_ACTION_ID);

	private CollaborationUtils() {
	}

	/** System project id for a playground room mode; null or blank is a normal playground room. */
	public static String projectIdForMode(String mode) {
		if (mode == null || mode.isBlank()) {
			return PlaygroundUtils.PLAYGROUND_PROJECT_ID;
		}
		if (!MODE_COLLABORATION.equals(mode.trim().toLowerCase())) {
			throw new IllegalArgumentException("Unknown room mode '" + mode + "'. Supported: " + MODE_COLLABORATION);
		}
		return COLLABORATION_PROJECT_ID;
	}

	public static boolean isCollaborationRoom(Room room) {
		return room != null && COLLABORATION_PROJECT_ID.equals(room.getProjectId());
	}
}
