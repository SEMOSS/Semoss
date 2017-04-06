package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.NounMetadata;
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
	
	public NounMetadata execute() {
		// get the name of the frame type
		String frameType = this.curRow.get(0).toString();
		// use factory to generate the new table
		ITableDataFrame newFrame = FrameFactory.getFrame(frameType);
		// store it as the result and push it to the planner to override
		// any existing frame that was in use
		NounMetadata result = new NounMetadata(newFrame, PkslDataTypes.FRAME);
		planner.addProperty("FRAME", "FRAME", newFrame);
		return result;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.FRAME);
		outputs.add(output);
		return outputs;
	}
}
