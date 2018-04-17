package prerna.sablecc2.reactor.task.modifiers;

import java.util.List;

import prerna.sablecc2.reactor.task.transformation.map.GenericMapTransformation;
import prerna.sablecc2.reactor.task.transformation.map.MapTransformationTask;

public class GenericMapLambdaTaskReactor extends AbstractLambdaTaskReactor {


	/**
	 * Abstract lambda class is responsible for getting
	 * data from the noun store / prop store
	 */

	public GenericMapLambdaTaskReactor() {
		this.keysToGet = new String[]{"CODE", IMPORTS_KEY};
	}

	@Override
	protected void buildTask() {
		String code = getCode();
		List<String> imports = getImports();

		// create the transformation
		// TODO: do this by reflection?
		GenericMapTransformation transformation = new GenericMapTransformation();
		try {
			transformation.init(code, imports);
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error with creating generic lambda!");			
		}

		// create a new task and add to stores
		MapTransformationTask newTask = new MapTransformationTask();
		newTask.setInnerTask(this.task);
		newTask.setTransformation(transformation);
		this.task = newTask;
		this.insight.getTaskStore().addTask(this.task);
	}

}
