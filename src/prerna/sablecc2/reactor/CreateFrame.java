package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class CreateFrame extends AbstractReactor {

	public NounMetadata execute() {
		// get the name of the frame type
		String frameType = this.curRow.get(0).toString();
		// use factory to generate the new table
		String alias = getAlias();
		ITableDataFrame newFrame = FrameFactory.getFrame(frameType, alias);
		
		NounMetadata noun = new NounMetadata(newFrame, PkslDataTypes.FRAME);
		
		// store it as the result and push it to the planner to override
		// any existing frame that was in use
		planner.addProperty("FRAME", "FRAME", newFrame);
		planner.getVarStore().put(Insight.CUR_FRAME_KEY, noun);

		return noun;
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
	
	/**
	 * Get an alias for the frame
	 * This doesn't have a meaning for all frame types
	 * But is useful for sql frames where we need to define a table name
	 * @return
	 */
	private String getAlias() {
		List<Object> alias = this.curRow.getColumnsOfType(PkslDataTypes.ALIAS);
		if(alias != null && alias.size() > 0) {
			return alias.get(0).toString();
		}
		return null;
	}

}
