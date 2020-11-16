package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;

public class BasicIteratorTaskAdapter extends AbstractSemossTypeAdapter<BasicIteratorTask> {

	static enum MODE {RECREATE_NEW, CONTINUE_PREVIOUS_ITERATING}
	private MODE curMode = MODE.RECREATE_NEW;
	
	@Override
	public BasicIteratorTask read(JsonReader in) throws IOException {
		String taskId = null;
		int numCollected = 0;
		long internalOffset = 0;
		TaskOptions tOptions = null;
		SelectQueryStruct qs = null;
		
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
			} else if(key.equals("numCollected")) {
				numCollected = (int) in.nextLong();
			}
			else if(key.equals("internalOffset")) {
				internalOffset = in.nextLong();
			} else if(key.equals("taskOptions")) {
				TaskOptionsAdapter adapter = new TaskOptionsAdapter();
				tOptions = (TaskOptions) adapter.read(in);
			} else if(key.equals("qs")) {
				SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
				adapter.setInsight(this.insight);
				qs = adapter.read(in);
			}
		}
		in.endObject();

		BasicIteratorTask task = new BasicIteratorTask(qs);
		task.setId(taskId);
		task.setTaskOptions(tOptions);
		task.setNumCollect(numCollected);
		task.setInternalOffset(internalOffset);
		if(curMode == MODE.CONTINUE_PREVIOUS_ITERATING) {
			task.setInternalOffset(internalOffset + numCollected);
		}
		
		return task;
	}

	@Override
	public void write(JsonWriter out, BasicIteratorTask value) throws IOException {
		out.beginObject();
		out.name("taskType").value("basic");
		out.name("id").value(value.getId());
		out.name("numCollected").value(value.getNumCollect());
		out.name("internalOffset").value(value.getInternalOffset());
		out.name("taskOptions");
		if(value.getTaskOptions() != null) {
			TaskOptionsAdapter adapter = new TaskOptionsAdapter();
			adapter.write(out, value.getTaskOptions());
		} else {
			out.nullValue();
		}
		out.name("qs");
		SelectQueryStruct qs = value.getQueryStruct();
		SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
		adapter.write(out, qs);
		out.endObject();
	}
	
	public void setCurMode(MODE curMode) {
		this.curMode = curMode;
	}

}
