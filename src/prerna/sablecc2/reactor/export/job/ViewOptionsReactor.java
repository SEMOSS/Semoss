package prerna.sablecc2.reactor.export.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PkslDataTypes;

public class ViewOptionsReactor extends JobBuilderReactor {

	@Override
	protected void buildJob() {
		List<Object> mapOptions = this.curRow.getColumnsOfType(PkslDataTypes.MAP);
		if(mapOptions == null || mapOptions.size() == 0) {
			// if it is null, i guess we just clear the map values
			job.setViewOptions(new HashMap<String, Object>());
		} else {
			job.setViewOptions((Map<String, Object>) mapOptions.get(0));
		}
	}
}
