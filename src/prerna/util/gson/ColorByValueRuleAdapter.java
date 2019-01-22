package prerna.util.gson;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.om.ColorByValueRule;
import prerna.query.querystruct.SelectQueryStruct;

public class ColorByValueRuleAdapter extends TypeAdapter<ColorByValueRule> {

	private static final Gson GSON = GsonUtility.getDefaultGson();
	private static final Gson SIMPLE_GSON = new Gson();

	@Override
	public void write(JsonWriter out, ColorByValueRule value) throws IOException {
		out.beginObject();

		out.name("id").value(value.getId());
		out.name("options").value(SIMPLE_GSON.toJson(value.getOptions()));
		out.name("qs").value(GSON.toJson(value.getQueryStruct()));
		
		out.endObject();
	}

	@Override
	public ColorByValueRule read(JsonReader in) throws IOException {
		String id = null;
		Map<String, Object> options = null;
		SelectQueryStruct qs = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("id")) {
				id = in.nextString();
			} else if(name.equals("options")) {
				options = GSON.fromJson(in.nextString(), Map.class);
			} else if(name.equals("qs")) {
				SelectQueryStructAdapter qsAdapter = new SelectQueryStructAdapter();
				qs = qsAdapter.read(in);
			}
		}
		
		return new ColorByValueRule(id, qs, options);
	}

}
