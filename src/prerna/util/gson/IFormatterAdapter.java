package prerna.util.gson;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.reactor.export.FormatFactory;
import prerna.reactor.export.IFormatter;

public class IFormatterAdapter extends AbstractSemossTypeAdapter<IFormatter> {

	private static Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public IFormatter read(JsonReader in) throws IOException {
		if(in.peek() == JsonToken.NULL) {
			return null;
		}
		
		String type = null;
		Map<String, Object> options = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			if(name.equals("type")) {
				type = in.nextString();
			}
			if(name.equals("options")) {
				TypeAdapter adapter = GSON.getAdapter(Map.class);
				options = (Map<String, Object>) adapter.read(in);
			}
		}
		in.endObject();
		
		IFormatter formatter = FormatFactory.getFormatter(type);
		formatter.setOptionsMap(options);
		return formatter;
	}
	
	@Override
	public void write(JsonWriter out, IFormatter value) throws IOException {
		if(value == null) {
			out.nullValue();
			return;
		}
		
		out.beginObject();
		out.name("type").value(value.getFormatType());
		out.name("options");
		Map<String, Object> options = value.getOptionsMap();
		if(options == null) {
			out.nullValue();
		} else {
			TypeAdapter adapter = GSON.getAdapter(options.getClass());
			adapter.write(out, options);
		}
		
		out.endObject();
	}
	
}
