package prerna.util.gson;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;

public class TaskStoreAdapter extends TypeAdapter<TaskStore> {

	private static final String BASIC = "basic";
	private static final String CONSTANT = "constant";
	
	private Insight insight;
	
	// get the panel id to task id
	// for the written task store
	private List<Map<String, String>> panelIdToTaskList;
	
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
		panelIdToTaskList = new Vector<Map<String, String>>();
		
		Set<String> tasks = value.getTaskIds();
		for(String taskId : tasks) {
			out.beginObject();
			ITask t = value.getTask(taskId);
			TypeAdapter adapter = null;
			if(t instanceof BasicIteratorTask) {
				out.name("type").value(BASIC);
				adapter = new BasicIteratorTaskAdapter();
			} else if(t instanceof ConstantDataTask) {
				out.name("type").value(CONSTANT);
				adapter = new ConstantDataTaskAdapter();
			}
			out.name("task");
			adapter.write(out, t); 

			// store the task to panel ids
			// note: this works because the tasks are stored in order
			TaskOptions taskOptions = t.getTaskOptions();
			if(taskOptions != null && !taskOptions.isEmpty()) {
				Set<String> panelIds = taskOptions.getPanelIds();
				for(String panelId : panelIds) {
					Map<String, String> panelToTask = new HashMap<String, String>();
					panelToTask.put(panelId, taskId);
					panelIdToTaskList.add(panelToTask);
				}
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
			in.beginObject();
			
			// get the type
			in.nextName();
			String type = in.nextString();
			
			// the next name is the task
			in.nextName();
			// now time to create the task
			ITask task = null;
			
			if(type.equals(BASIC)) {
				BasicIteratorTaskAdapter adapter = new BasicIteratorTaskAdapter();
				adapter.setCurMode(BasicIteratorTaskAdapter.MODE.CONTINUE_PREVIOUS_ITERATING);
				task = adapter.read(in);
				SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
				// need to set the source
				IEngine engine = qs.retrieveQueryStructEngine();
				// is it an engine
				if(engine != null) {
					qs.setEngine(engine);
				} else if(this.insight != null) {
					// not an engine
					// must be a frame
					// see if we can identify the variable
					String frameName = qs.getFrameName();
					if(frameName != null) {
						NounMetadata frame = insight.getVarStore().get(frameName);
						if(frame != null) {
							qs.setFrame( (ITableDataFrame) frame.getValue());
						} else {
							qs.setFrame( (ITableDataFrame) insight.getDataMaker());
						}
					} else {
						qs.setFrame( (ITableDataFrame) insight.getDataMaker());
					}
				}
			} else if(type.equals(CONSTANT)) {
				ConstantDataTaskAdapter adapter = new ConstantDataTaskAdapter();
				task = adapter.read(in);
			} else {
				// you messed up
			}
			
			// end the task object
			in.endObject();
			
			// store the task
			tStore.addTask(task.getId(), task);
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
	
	/**
	 * This is only set when we are writing
	 * This will be null if you are reading
	 * @return
	 */
	public List<Map<String, String>> getPanelIdToTask() {
		return this.panelIdToTaskList;
	}
}
