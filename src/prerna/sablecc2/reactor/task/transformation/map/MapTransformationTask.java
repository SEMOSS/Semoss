package prerna.sablecc2.reactor.task.transformation.map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.AbstractTaskOperation;

public class MapTransformationTask extends AbstractTaskOperation {

	private IMapTransformation transform;

	@Override
	public IHeadersDataRow next() {
		IHeadersDataRow row = this.innerTask.next();
		return transform.process(row);
	}

	public void setTransformation(IMapTransformation transformation) {
		this.transform = transformation;
	}
	
}
