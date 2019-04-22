package prerna.util.gson;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.JdbcHardSelectQueryStruct;

public class JdbcHardSelectQueryStructAdapter  extends TypeAdapter<JdbcHardSelectQueryStruct> {

	private static final Gson gson = new Gson();

	@Override
	public JdbcHardSelectQueryStruct read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		JdbcHardSelectQueryStruct qs = new JdbcHardSelectQueryStruct();

		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("qsType")) {
				qs.setQsType(QUERY_STRUCT_TYPE.valueOf(in.nextString()));
			} else if(name.equals("engineName")) {
				qs.setEngineId(in.nextString());
			} else if(name.equals("frameName")) {
				qs.setFrameName(in.nextString());
			} else if(name.equals("query")) {
				qs.setQuery(in.nextString());
			} else if(name.equals("config")) {
				TypeAdapter<Map> configReader = gson.getAdapter(Map.class);
				Map<String, Object> config = configReader.read(in);
				qs.setConfig(config);
			}
		}
		in.endObject();

		return qs;
	}
	
	@Override
	public void write(JsonWriter out, JdbcHardSelectQueryStruct value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// this will be fun...
		// will try to go ahead and write everything

		out.beginObject();

		// lets do the easy ones first
		// qs type
		out.name("qsType").value(value.getQsType().toString());
		if(value.getEngineId() != null) {
			out.name("engineName").value(value.getEngineId());
		}
		if(value.getFrameName() != null) {
			out.name("frameName").value(value.getFrameName());
		}
		out.name("query").value(value.getQuery());
		
		if(value.getConfig() != null) {
			TypeAdapter<Map> configWriter = gson.getAdapter(Map.class);
			out.name("config");
			configWriter.write(out, value.getConfig());
		}
		out.endObject();
	}
}
