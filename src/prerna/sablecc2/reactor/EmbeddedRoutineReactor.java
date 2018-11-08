package prerna.sablecc2.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EmbeddedRoutineReactor extends AbstractReactor {
	
	// this class does nothing!
	// it is meant when we have an embedded routine 
	// within our main routine
	// but i just want to collect all the output in one location
	
	@Override
	public NounMetadata execute() {
		return new NounMetadata(this.curRow.getVector(), PixelDataType.VECTOR);
	}
}