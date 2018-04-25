package prerna.sablecc2.reactor.task.lambda.map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.AbstractTaskOperation;

public class MapLambdaTask extends AbstractTaskOperation {

	private IMapLambda lambda;

	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow row = this.innerTask.next();
		return lambda.process(row);
	}

	public void setLambda(IMapLambda lambda) {
		this.lambda = lambda;
	}
	
}
