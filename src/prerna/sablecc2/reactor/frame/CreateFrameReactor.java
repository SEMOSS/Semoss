package prerna.sablecc2.reactor.frame;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class CreateFrameReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateFrameReactor.class.getName();
	
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// get the name of the frame type
		String frameType = this.curRow.get(0).toString();
		// use factory to generate the new table
		String alias = getAlias();
		if(alias == null) {
			logger.info("Creating new frame of type = " + frameType + " with no alias");
			alias = "";
		} else {
			logger.info("Creating new frame of type = " + frameType + " with alias = " + alias);
		}
		ITableDataFrame newFrame = FrameFactory.getFrame(frameType, alias);
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		
		// store it as the result and push it to the planner to override
		// any existing frame that was in use
		planner.addProperty("FRAME", "FRAME", newFrame);
		planner.getVarStore().put(Insight.CUR_FRAME_KEY, noun);

		return noun;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) {
			return outputs;
		}
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FRAME, PixelOperationType.FRAME);
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
		List<Object> alias = this.curRow.getValuesOfType(PixelDataType.ALIAS);
		if(alias != null && alias.size() > 0) {
			return alias.get(0).toString();
		}
		return null;
	}

}
