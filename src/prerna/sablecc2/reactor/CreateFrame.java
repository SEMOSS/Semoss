package prerna.sablecc2.reactor;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PkslDataTypes;

public class CreateFrame extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	public Object execute() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		String frameType = (String)allNouns.get(0).toString().toUpperCase();
		ITableDataFrame newFrame = FrameFactory.getFrame(frameType);
		NounMetadata result = new NounMetadata(newFrame, PkslDataTypes.FRAME);
		planner.addProperty("FRAME", "FRAME", newFrame);
		return result;
	}

	@Override
	public void mergeUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updatePlan() {
		// TODO Auto-generated method stub
		
	}

}
