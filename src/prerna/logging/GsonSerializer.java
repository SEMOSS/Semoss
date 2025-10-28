package prerna.logging;



import java.time.ZoneOffset;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.impl.model.Room;
import prerna.util.gson.RoomAdapter;
import prerna.util.gson.ZoneOffsetTypeAdapter;

public class GsonSerializer {
	
	private static final Gson GSON =  new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeAdapter(Room.class, new RoomAdapter())
			.registerTypeAdapter(ZoneOffset.class, new ZoneOffsetTypeAdapter())
			.create();
	
	public static String toJson(Object obj) {
		return GSON.toJson(obj);
	}
}
