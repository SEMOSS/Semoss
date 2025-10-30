package prerna.util.gson;

import java.io.IOException;
import java.time.ZoneOffset;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class ZoneOffsetTypeAdapter extends TypeAdapter<ZoneOffset> {

	@Override
	public void write(JsonWriter out, ZoneOffset value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		out.value(value.getId());
	}

	@Override
	public ZoneOffset read(JsonReader in) throws IOException {
		if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		return ZoneOffset.of(in.nextString());
	}

}
