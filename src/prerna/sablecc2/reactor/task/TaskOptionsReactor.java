package prerna.sablecc2.reactor.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;

public class TaskOptionsReactor extends TaskBuilderReactor {

	@Override
	protected void buildTask() {
		List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
		if(mapOptions == null || mapOptions.size() == 0) {
			// if it is null, i guess we just clear the map values
			this.task.setTaskOptions(new HashMap<String, Object>());
		} else {
			this.task.setTaskOptions((Map<String, Object>) mapOptions.get(0));
		}
	}
}
