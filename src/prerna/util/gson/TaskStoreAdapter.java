package prerna.util.gson;

import java.io.IOException;
import java.util.Set;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;

public class TaskStoreAdapter extends AbstractSemossTypeAdapter<TaskStore> {

	private static final String BASIC = "basic";
	private static final String CONSTANT = "constant";
	
	@Override
	public void write(JsonWriter out, TaskStore value) throws IOException {
		out.beginObject();
		
		out.name("tasks");
		out.beginArray();
		
		Set<String> tasks = value.getTaskIds();
		for(String taskId : tasks) {
			out.beginObject();
			ITask t = value.getTask(taskId);
			
			if(t instanceof BasicIteratorTask) {
				out.name("type").value(BASIC);
				out.name("task");
				BasicIteratorTaskAdapter adapter = new BasicIteratorTaskAdapter();
				adapter.write(out, (BasicIteratorTask) t); 
			} else if(t instanceof ConstantDataTask) {
				out.name("type").value(CONSTANT);
				out.name("task");
				ConstantDataTaskAdapter adapter = new ConstantDataTaskAdapter();
				adapter.write(out, (ConstantDataTask) t); 
			}
			
			out.name("taskOptions");
			if(t.getTaskOptions() != null) {
				TaskOptionsAdapter adapter = new TaskOptionsAdapter();
				adapter.write(out, t.getTaskOptions());
			} else {
				out.nullValue();
			}
			
			out.endObject();
		}
		out.endArray();
		
		// also output the counter
		out.name("taskCounter").value(value.getCount());
		out.endObject();
	}

	@Override
	public TaskStore read(JsonReader in) throws IOException {
		TaskStore tStore = new TaskStore();
		
		in.beginObject();
		
		// get the tasks
		in.nextName();
		// we have a list
		in.beginArray();
		while(in.hasNext()) {
			
			String taskType = null;
			ITask task = null;
			TaskOptions taskOptions = null;
			
			// start the object for the task
			in.beginObject();
			while(in.hasNext()) {
				String key = in.nextName();
				if(in.peek() == JsonToken.NULL) {
					in.nextNull();
					continue;
				}
				
				if(key.equals("type")) {
					taskType = in.nextString();
					
				} else if(key.equals("task")) {
					if(taskType.equals(BASIC)) {
						BasicIteratorTaskAdapter adapter = new BasicIteratorTaskAdapter();
						adapter.setInsight(this.insight);
						adapter.setCurMode(BasicIteratorTaskAdapter.MODE.CONTINUE_PREVIOUS_ITERATING);
						task = adapter.read(in);
//						SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
//						// need to set the source
//						IEngine engine = qs.retrieveQueryStructEngine();
//						// is it an engine
//						if(engine != null) {
//							qs.setEngine(engine);
//						} else if(this.insight != null) {
//							// not an engine
//							// must be a frame
//							// see if we can identify the variable
//							String frameName = qs.getFrameName();
//							if(frameName != null) {
//								NounMetadata frame = insight.getVarStore().get(frameName);
//								if(frame != null) {
//									qs.setFrame( (ITableDataFrame) frame.getValue());
//								} else {
//									qs.setFrame( (ITableDataFrame) insight.getDataMaker());
//								}
//							} else {
//								qs.setFrame( (ITableDataFrame) insight.getDataMaker());
//							}
//						}
					} else if(taskType.equals(CONSTANT)) {
						ConstantDataTaskAdapter adapter = new ConstantDataTaskAdapter();
						task = adapter.read(in);
					}
					
				} else if(key.equals("taskOptions")){
					TaskOptionsAdapter adapter = new TaskOptionsAdapter();
					taskOptions = adapter.read(in);
					
				}
			}
			in.endObject();
			
			// store the task
			// with the task options
			if (task != null) {
				task.setTaskOptions(taskOptions);
				tStore.addTask(task.getId(), task);
			}
		}
		// end the tasks array
		in.endArray();
		
		// get the count
		in.nextName();
		long taskCount = in.nextLong();
		tStore.setCount(taskCount);
		
		// end the task store
		in.endObject();
		return tStore;
	}
}
