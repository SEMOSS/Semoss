package prerna.reactor.frame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.AbstractReactor;
import prerna.reactor.EmbeddedRoutineReactor;
import prerna.reactor.EmbeddedScriptReactor;
import prerna.reactor.GenericReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.AbstractSqlQueryUtil;

public class CreateFrameReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateFrameReactor.class.getName();
	private static final String OVERRIDE = "override";
	
	public CreateFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME_TYPE.getKey(), OVERRIDE, ReactorKeysEnum.ALIAS.getKey()};
	}
	
	public NounMetadata execute() {
		organizeKeys();
		// get the name of the frame type
		String frameType = this.keyValue.get(this.keysToGet[0]);
		// override the default frame
		Boolean override = true;
		String overrideStr = this.keyValue.get(this.keysToGet[1]);
		if(overrideStr != null && !overrideStr.isEmpty()) {
			override = Boolean.parseBoolean(overrideStr);
		}
		// set the alias for the frame
		String alias = this.keyValue.get(this.keysToGet[2]);

		Logger logger = getLogger(CLASS_NAME);
		if(alias == null || alias.trim().isEmpty()) {
			logger.info("Creating new frame of type = " + frameType + " with a random alias");
			alias = "";
		} else {
			// clean the alias - make alpha numeric underscore + not start with a digit
			try {
				alias = AbstractSqlQueryUtil.cleanTableName(alias);
				logger.info("Creating new frame of type = " + frameType + " with alias = " + alias);
			} catch(Exception e) {
				alias = "";
				logger.info("Invalid alias - creating new frame of type = " + frameType + " with a random alias");
			}
		}
		ITableDataFrame newFrame = null;
		try {
			newFrame = FrameFactory.getFrame(this.insight, frameType, alias);
		} catch (Exception e) {
			String message = "Error occurred trying to create frame of type " + frameType;
			String cause = e.getMessage();
			if(cause != null && !cause.isEmpty()) {
				message += ". Detailed error message = " + cause;
			}
			throw new IllegalArgumentException(message, e);
		}
		logger.info("Frame " + newFrame.getName() + " created");

		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		// store it as the result and push it to the planner to override
		// any existing frame that was in use
		if(override) {
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
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(OVERRIDE)) {
			return "Indicates if the current frame should be overridden; default value of true";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	
	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		if(parentReactor != null) {
			// try to merge the actual frame
			// else, merge the frame map
			organizeKeys();
			String alias = this.keyValue.get(this.keysToGet[2]);
			NounMetadata data = null;
			if(alias != null && !alias.isEmpty()) {
				data = this.insight.getVarStore().get(alias);
			}
			if(data == null) {
				Map<String, List<Map>> map = getStoreMap();
				map.put("createFrame", new ArrayList<>());
				data = new NounMetadata(map, PixelDataType.FRAME, PixelOperationType.FRAME_MAP);
			}
	    	if(parentReactor instanceof EmbeddedScriptReactor || parentReactor instanceof EmbeddedRoutineReactor
	    			|| parentReactor instanceof GenericReactor) {
	    		parentReactor.getCurRow().add(data);
	    	} else {
	    		GenRowStruct parentInput = parentReactor.getNounStore().makeNoun(PixelDataType.FRAME.getKey());
				parentInput.add(data);
	    	}
		}
	}

}
