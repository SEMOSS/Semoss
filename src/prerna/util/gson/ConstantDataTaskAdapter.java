package prerna.util.gson;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;

public class ConstantDataTaskAdapter extends AbstractSemossTypeAdapter<ConstantDataTask> {
	
	private static final Logger classLogger = LogManager.getLogger(ConstantDataTaskAdapter.class);

	private static final Gson GSON = new Gson();

	@Override
	public ConstantDataTask read(JsonReader in) throws IOException {
		String taskId = null;
		TaskOptions tOptions = null;
		String objStr = null;
		String objClass = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			JsonToken peek = in.peek();
			if(peek == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			if(key.equals("taskType")) {
				in.nextString();
			} else if(key.equals("id")) {
				taskId = in.nextString();
			} else if(key.equals("taskOptions")) {
				tOptions = GSON.fromJson(in.nextString(), TaskOptions.class);
			} else if(key.equals("outputData")) {
				objStr = in.nextString();
			} else if(key.equals("outputDataClass")) {
				objClass = in.nextString();
			}
		}
		in.endObject();

		ConstantDataTask task = new ConstantDataTask();
		task.setId(taskId);
		task.setTaskOptions(tOptions);

		Object outputData = null;
		try {
			Class c = Class.forName(objClass);
			outputData = GSON.fromJson(objStr, c);
			task.setOutputData(outputData);;
		} catch (ClassNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return task;
	}
	
	@Override
	public void write(JsonWriter out, ConstantDataTask value) throws IOException {
		out.beginObject();
		out.name("taskType").value("constant");
		out.name("id").value(value.getId());
		out.name("taskOptions").value(GSON.toJson(value.getTaskOptions()));
		// also store all the data
		Object data = value.getData();
		out.name("outputData").value(GSON.toJson(data));
		out.name("outputDataClass").value(data.getClass().getName());
		out.endObject();
	}

}	
