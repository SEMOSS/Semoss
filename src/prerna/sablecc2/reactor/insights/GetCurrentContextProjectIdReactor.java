package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

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
