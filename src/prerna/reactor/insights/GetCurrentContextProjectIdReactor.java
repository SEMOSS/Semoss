package prerna.reactor.insights;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCurrentContextProjectIdReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		String projectContextId = this.insight.getContextProjectId();
		if(projectContextId == null) {
			return new NounMetadata(null, PixelDataType.NULL_VALUE);
		}
		return new NounMetadata(projectContextId, PixelDataType.CONST_STRING);
	}

}
