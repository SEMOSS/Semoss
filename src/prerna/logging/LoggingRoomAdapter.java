package prerna.logging;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import prerna.engine.impl.model.Room;

public class LoggingRoomAdapter implements JsonSerializer<Room> {

	@Override
	public JsonElement serialize(Room src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("roomId", src.getId());
		jsonObject.addProperty("roomName", src.getRoomName());
		return jsonObject;
	}
}
