package prerna.util.gson;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.sablecc2.om.task.options.TaskOptions;

public class TaskOptionsAdapter extends AbstractSemossTypeAdapter<TaskOptions> {

	private static Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public TaskOptions read(JsonReader in) throws IOException {
		if(in.peek() == JsonToken.NULL) {
			return null;
		}
		
		Map<String, Object> options = null;
		boolean isOrnament = false;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
			}
			
			if(name.equals("options")) {
				TypeAdapter adapter = GSON.getAdapter(Map.class);
				options = (Map<String, Object>) adapter.read(in);
				
			} else if(name.equals("ornament")){
				isOrnament = in.nextBoolean();
			}
			
		}
		in.endObject();
		
		TaskOptions taskOptions = new TaskOptions(options);
		taskOptions.setOrnament(isOrnament);
		return taskOptions;
	}
	
	@Override
	public void write(JsonWriter out, TaskOptions value) throws IOException {
		if(value == null) {
			out.nullValue();
			return;
		}
		
		out.beginObject();
		out.name("options");
		Map<String, Object> options = value.getOptions();
		if(options == null) {
			out.nullValue();
		} else {
			TypeAdapter adapter = GSON.getAdapter(options.getClass());
			adapter.write(out, options);
		}
		out.name("ornament");
		out.value(value.isOrnament());
		
		out.endObject();
	}
	
}
