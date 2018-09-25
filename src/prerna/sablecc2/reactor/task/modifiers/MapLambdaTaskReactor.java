package prerna.sablecc2.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.reactor.task.lambda.map.IMapLambda;
import prerna.sablecc2.reactor.task.lambda.map.MapLambdaTask;
import prerna.sablecc2.reactor.task.lambda.map.function.MapLambdaFactory;

public class MapLambdaTaskReactor extends TaskBuilderReactor {

	/**
	 * Allow you to modidy an existing column(s) or add new columns
	 * Will not allow you to add new rows
	 */
	
	public MapLambdaTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.LAMBDA.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.PARAM_KEY.getKey()};
	}
	
	@Override
	protected void buildTask() {
		String lambda = getLambda();
		List<String> columns = getColumns();
		Map params = getMap();

		IMapLambda mapLambda = MapLambdaFactory.getLambda(lambda);
		if(mapLambda == null) {
			throw new IllegalArgumentException("Unknown transformation type = " + lambda);
		}
		mapLambda.setUser(this.insight.getUser());
		mapLambda.setParams(params);
		mapLambda.init(this.task.getHeaderInfo(), columns);
		
		// create a new task and add to stores
		MapLambdaTask newTask = new MapLambdaTask();
		newTask.setInnerTask(this.task);
		newTask.setLambda(mapLambda);
		newTask.setHeaderInfo(mapLambda.getModifiedHeaderInfo());

		this.task = newTask;
		this.insight.getTaskStore().addTask(this.task);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////
	
	// inputs
	
	private String getLambda() {
		GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
		if(colGrs != null && !colGrs.isEmpty()) {
			return colGrs.get(0).toString();
		}
		
		throw new IllegalArgumentException("No transformation type was entered");
	}
	
	private List<String> getColumns() {
		GenRowStruct colGrs = this.store.getNoun(keysToGet[1]);
		if(colGrs != null && !colGrs.isEmpty()) {
			int size = colGrs.size();
			List<String> columns = new ArrayList<String>();
			for(int i = 0; i < size; i++) {
				columns.add(colGrs.get(i).toString());
			}
			return columns;
		}
		
		return null;
	}
	
	private Map getMap() {
		GenRowStruct colGrs = this.store.getNoun(keysToGet[2]);
		if(colGrs != null && !colGrs.isEmpty()) {
			return (Map) colGrs.get(0);
		}
		
		return null;
	}
}
