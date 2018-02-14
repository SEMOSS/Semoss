package prerna.sablecc2.reactor.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.ga.GATracker;

public class TaskOptionsReactor extends TaskBuilderReactor {
	
	public TaskOptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.OPTIONS.getKey()};
	}

	@Override
	protected void buildTask() {
		List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
		if(mapOptions == null || mapOptions.size() == 0) {
			// if it is null, i guess we just clear the map values
			this.task.setTaskOptions(new HashMap<String, Object>());
		} else {
			this.task.setTaskOptions((Map<String, Object>) mapOptions.get(0));
		}
		
		// track GA data
		if (this.task instanceof BasicIteratorTask) {
			try {
				GATracker.getInstance().trackViz(this.task.getTaskOptions(), this.insight, ((BasicIteratorTask) task).getQueryStruct());
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
}
