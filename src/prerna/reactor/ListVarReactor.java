package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListVarReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(insight.getAllVars(), PixelDataType.VECTOR);
	}

}
