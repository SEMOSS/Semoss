package prerna.sablecc2.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.reactor.task.lambda.map.MapLambdaTask;
import prerna.sablecc2.reactor.task.lambda.map.ToNumericTypeLambda;

public class ToNumericTypeTaskReactor extends TaskBuilderReactor {

	public ToNumericTypeTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	@Override
	protected void buildTask() {
		// get the columns
		List<String> cols = getColumns();
		
		// create a new task and add to stores
		MapLambdaTask newTask = new MapLambdaTask();
		newTask.setInnerTask(this.task);
		ToNumericTypeLambda transformation = new ToNumericTypeLambda();
		transformation.init(this.task.getHeaderInfo(), cols);
		newTask.setTransformation(transformation);
		newTask.setHeaderInfo(transformation.getModifiedHeaderInfo());
		this.task = newTask;
		this.insight.getTaskStore().addTask(this.task);
	}
	
	private List<String> getColumns() {
		GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
		if(colGrs != null && !colGrs.isEmpty()) {
			int size = colGrs.size();
			List<String> columns = new ArrayList<String>();
			for(int i = 0; i < size; i++) {
				columns.add(colGrs.get(i).toString());
			}
			return columns;
		}
		
		List<String> columns = new ArrayList<String>();
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			columns.add(this.curRow.get(i).toString());
		}
		return columns;
	}
}
