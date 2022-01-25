package prerna.sablecc2.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListVarReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// real simple
		// get all the vars as a vector
		// pump it out
		return new NounMetadata(insight.getAllVars(), PixelDataType.VECTOR);
	}

}
