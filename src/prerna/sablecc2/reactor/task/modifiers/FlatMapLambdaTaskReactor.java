package prerna.sablecc2.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.reactor.task.lambda.flatmap.FlatMapLambdaFactory;
import prerna.sablecc2.reactor.task.lambda.flatmap.FlatMapLambdaTask;
import prerna.sablecc2.reactor.task.lambda.flatmap.IFlatMapLambda;

public class FlatMapLambdaTaskReactor extends TaskBuilderReactor {

	/**
	 * Allow you to modidy an existing column(s) or add new columns
	 * Will not allow you to add new rows
	 */
	
	public FlatMapLambdaTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.LAMBDA.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	@Override
	protected void buildTask() {
		String lambda = getLambda();
		List<String> columns = getColumns();
		
		IFlatMapLambda mapLambda = FlatMapLambdaFactory.getLambda(lambda);
		if(mapLambda == null) {
			throw new IllegalArgumentException("Unknown transformation type");
		}
		mapLambda.setUser(this.insight.getUser());
		mapLambda.init(this.task.getHeaderInfo(), columns);
		
		// create a new task and add to stores
		FlatMapLambdaTask newTask = new FlatMapLambdaTask();
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
		
		throw new IllegalArgumentException("Unknown transformation type");
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
}
