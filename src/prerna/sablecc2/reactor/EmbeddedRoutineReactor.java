package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EmbeddedRoutineReactor extends AbstractReactor {
	
	// this class does nothing!
	// it is meant when we have an embedded routine 
	// within our main routine
	// but i just want to collect all the output in one location
	
	@Override
	public NounMetadata execute() {
		List<NounMetadata> nList = new Vector<NounMetadata>();
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			NounMetadata n = this.curRow.getNoun(i);
			if(n.getNounType() == PixelDataType.LAMBDA) {
				n = ((IReactor) n.getValue()).execute();
			}
			nList.add(n);
		}
		return new NounMetadata(nList, PixelDataType.VECTOR, PixelOperationType.SUB_SCRIPT);
	}
}