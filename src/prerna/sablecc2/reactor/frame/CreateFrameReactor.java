package prerna.sablecc2.reactor.frame;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class CreateFrameReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateFrameReactor.class.getName();
	private static final String OVERRIDE = "override";
	
	public CreateFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME_TYPE.getKey(), OVERRIDE};
	}
	
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// get the name of the frame type
		String frameType = getFrameType();
		// use factory to generate the new table
		String alias = getAlias();
		if(alias == null) {
			logger.info("Creating new frame of type = " + frameType + " with no alias");
			alias = "";
		} else {
			logger.info("Creating new frame of type = " + frameType + " with alias = " + alias);
		}
		ITableDataFrame newFrame = FrameFactory.getFrame(this.insight, frameType, alias);
		
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		// store it as the result and push it to the planner to override
		// any existing frame that was in use
		if(overrideFrame()) {
			this.insight.setDataMaker(newFrame);
		}
		// add the alias as a noun by default
		if(alias != null && !alias.isEmpty()) {
			this.insight.getVarStore().put(alias, noun);
		} else {
			// even if we cannot create an alias for the frame
			// always add it as the default
			this.insight.getVarStore().put(newFrame.getName(), noun);
		}
		
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
	 * Get the frame type
	 * @return
	 */
	private String getFrameType() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		return this.curRow.get(0).toString();
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
	
	private boolean overrideFrame() {
		GenRowStruct overrideGrs = this.store.getNoun(OVERRIDE);
		if(overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(OVERRIDE)) {
			return "Indicates if the current frame should be overridden; default value of true";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
