package prerna.reactor.task.modifiers;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.task.lambda.map.GenericMapLambda;
import prerna.reactor.task.lambda.map.MapLambdaReactor;
import prerna.util.Constants;

public class CodeLambdaReactor extends AbstractLambdaTaskReactor {

	private static final Logger classLogger = LogManager.getLogger(CodeLambdaReactor.class);

	/**
	 * Abstract lambda class is responsible for getting
	 * data from the noun store / prop store
	 */

	public CodeLambdaReactor() {
		this.keysToGet = new String[]{"CODE", IMPORTS_KEY};
	}

	@Override
	protected void buildTask() {
		String code = getCode();
		List<String> imports = getImports();

		// create the transformation
		// TODO: do this by reflection?
		GenericMapLambda lambda = new GenericMapLambda();
		try {
			lambda.init(code, imports);
			lambda.setUser(this.insight.getUser());
		} catch (InstantiationException | IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error with creating generic lambda!");			
		}

		// create a new task and add to stores
		MapLambdaReactor newTask = new MapLambdaReactor();
		newTask.setInnerTask(this.task);
		newTask.setLambda(lambda);
		this.task = newTask;
		this.insight.getTaskStore().addTask(this.task);
	}

}
