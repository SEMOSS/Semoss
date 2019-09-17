package prerna.sablecc2.reactor.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetFramesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> frameNames = new Vector<String>();
		Set<ITableDataFrame> uniqueFrames = new HashSet<ITableDataFrame>();
		
		VarStore varStore = this.insight.getVarStore();
		for(String k : varStore.getKeys()) {
			NounMetadata noun = varStore.get(k);
			if(noun.getNounType() == PixelDataType.FRAME) {
				uniqueFrames.add( (ITableDataFrame) noun.getValue());
			}
		}
		
		for(ITableDataFrame f : uniqueFrames) {
			frameNames.add(f.getName());
		}
		
		return new NounMetadata(frameNames, PixelDataType.CONST_STRING);
	}

}
