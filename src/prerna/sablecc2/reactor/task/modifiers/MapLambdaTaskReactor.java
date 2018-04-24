package prerna.sablecc2.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.reactor.task.transformation.map.IMapTransformation;
import prerna.sablecc2.reactor.task.transformation.map.MapTransformationTask;
import prerna.sablecc2.reactor.task.transformation.map.MapTransformations;

public class MapLambdaTaskReactor extends TaskBuilderReactor {

	/**
	 * Allow you to modidy an existing column(s) or add new columns
	 * Will not allow you to add new rows
	 */
	
	public MapLambdaTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TRANSFORMATION.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	@Override
	protected void buildTask() {
		String transformationName = getTransformation();
		List<String> columns = getColumns();
		
		IMapTransformation trans = MapTransformations.getTransformation(transformationName);
		if(trans == null) {
			throw new IllegalArgumentException("Unknown transformation type");
		}
		trans.setUser2(this.insight.getUser2());
		trans.init(this.task.getHeaderInfo(), columns);
		
		// create a new task and add to stores
		MapTransformationTask newTask = new MapTransformationTask();
		newTask.setInnerTask(this.task);
		newTask.setTransformation(trans);
		newTask.setHeaderInfo(trans.getModifiedHeaderInfo());

		this.task = newTask;
		this.insight.getTaskStore().addTask(this.task);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////
	
	// inputs
	
	private String getTransformation() {
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
