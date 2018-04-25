package prerna.sablecc2.reactor.task.lambda.map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.AbstractTaskOperation;

public class MapLambdaTask extends AbstractTaskOperation {

	private IMapLambda transform;

	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow row = this.innerTask.next();
		return transform.process(row);
	}

	public void setTransformation(IMapLambda transformation) {
		this.transform = transformation;
	}
	
}
