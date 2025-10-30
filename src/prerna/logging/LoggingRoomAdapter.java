package prerna.logging;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.engine.impl.model.Room;

public class LoggingRoomAdapter extends TypeAdapter<Room> {

	@Override
	public Room read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		Room room = new Room();
		in.beginObject();
		in.nextName();
		// engine id, engine name, rdbms id
		String roomId = in.nextString();
		room.setId(roomId);
		in.nextName();
		String projectId = in.nextString();
		room.getInsight().setContextProjectId(projectId);
		in.nextName();
		String projectName = in.nextString();
		room.getInsight().setContextProjectName(projectName);

		return room;
	}

	@Override
	public void write(JsonWriter out, Room room) throws IOException {
		if (room == null) {
			out.nullValue();
			return;
		}

		out.beginObject();
		out.name(SemossLogUtils.ROOM_ID).value(room.getId());
		out.name(SemossLogUtils.PROJECT_ID).value(room.getInsight().getContextProjectId());
		out.name(SemossLogUtils.PROJECT_NAME).value(room.getInsight().getContextProjectName());
		out.endObject();
	}
}
