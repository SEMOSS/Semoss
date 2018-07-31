package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class BasicIteratorTaskAdapter extends TypeAdapter<BasicIteratorTask> {

	@Override
	public BasicIteratorTask read(JsonReader in) throws IOException {
		in.beginObject();
		
		// this is the name and the value of basic
		in.nextName();
		in.nextString();
		
		// this is the task id
		in.nextName();
		String id = in.nextString();
		
		in.nextName();
		QueryStructAdapter adapter = new QueryStructAdapter();
		SelectQueryStruct qs = adapter.read(in);
		
		in.endObject();
		
		BasicIteratorTask task = new BasicIteratorTask(qs);
		task.setId(id);
		return task;
	}

	@Override
	public void write(JsonWriter out, BasicIteratorTask value) throws IOException {
		out.beginObject();
		out.name("taskType").value("basic");
		out.name("id").value(value.getId());
		out.name("qs");
		SelectQueryStruct qs = value.getQueryStruct();
		QueryStructAdapter adapter = new QueryStructAdapter();
		adapter.write(out, qs);
		out.endObject();
	}

}
