package prerna.util.gson;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;

public class TaskStoreAdapter extends TypeAdapter<TaskStore> {

	private Insight insight;
	
	// get the panel id to task id
	// for the written task store
	private Map<String, String> panelIdToTask;
	
	public TaskStoreAdapter() {
		
	}
	
	public TaskStoreAdapter(Insight insight) {
		this.insight = insight;
	}
	
	@Override
	public void write(JsonWriter out, TaskStore value) throws IOException {
		out.beginObject();
		
		out.name("tasks");
		out.beginArray();
		// i am also going to store
		// a task id to panel map
		// which will be used for the json cache of the view
		panelIdToTask = new HashMap<String, String>();
		
		Set<String> tasks = value.getTaskIds();
		for(String taskId : tasks) {
			ITask t = value.getTask(taskId);
			if(t instanceof BasicIteratorTask) {
				BasicIteratorTaskAdapter adapter = new BasicIteratorTaskAdapter();
				adapter.write(out, (BasicIteratorTask) t); 
			} else if(t instanceof ConstantDataTask) {
				ConstantDataTaskAdapter adapter = new ConstantDataTaskAdapter();
				adapter.write(out, (ConstantDataTask) t); 
			}
			
			// store the task to panel ids
			// note: this works because the tasks are stored in order
			TaskOptions taskOptions = t.getTaskOptions();
			if(taskOptions != null && !taskOptions.isEmpty()) {
				Set<String> panelIds = taskOptions.getPanelIds();
				for(String panelId : panelIds) {
					panelIdToTask.put(panelId, taskId);
				}
			}
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
		in.beginArray();
		while(in.hasNext()) {
			BasicIteratorTaskAdapter adapter = new BasicIteratorTaskAdapter();
			adapter.setCurMode(BasicIteratorTaskAdapter.MODE.CONTINUE_PREVIOUS_ITERATING);
			BasicIteratorTask t = adapter.read(in);
			SelectQueryStruct qs = t.getQueryStruct();
			try {
				IEngine engine = qs.retrieveQueryStructEngine();
				if(engine == null && this.insight != null) {
					// this means we cached a task that is using the frame
					// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
					// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
					// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
					// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
					qs.setFrame( (ITableDataFrame) insight.getDataMaker()); 
				}
			} catch(Exception e) {
				// this means we cached a task that is using the frame
				// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
				// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
				// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
				// TODO: NEED THE QS TO START TO HOLD THE FRAME NAME!!!!
				if(this.insight != null) {
					qs.setFrame( (ITableDataFrame) insight.getDataMaker()); 
				}
			}
		}
		in.endArray();
		
		// get the count
		in.nextName();
		long taskCount = in.nextLong();
		tStore.setCount(taskCount);
		
		in.endObject();
		
		return tStore;
	}
	
	/**
	 * This is only set when we are writing
	 * This will be null if you are reading
	 * @return
	 */
	public Map<String, String> getPanelIdToTask() {
		return this.panelIdToTask;
	}
}
