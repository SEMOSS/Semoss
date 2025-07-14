package prerna.reactor.frame.convert;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


/*
 * 
 * Types of Frames: R, native, python, 
 * Type of frame to generate
 *  grid (sql based frame)
 *   graph (frame based on tinkerpop)
 *    r (data sits within r, must have r installed to use)
 *     native (leverages the database to execute queries)
 *     
 *  Neel 
 * */

public class FrameToGraphReactor extends AbstractReactor {

	private static final String CLASS_NAME = FrameToGraphReactor.class.getName();
	
	public FrameToGraphReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.MODEL.getKey(), modelId };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		// set the logger into the frames
		Logger logger = getLogger(CLASS_NAME);
		ITableDataFrame sourceFrame = getSourceFrame(); // TODO: Remove this function and use the protected function in MergeFrames if this stays in the frame package
		sourceFrame.setLogger(logger);
		
		
		// Parse the sourceFrame
		
		// Prompt = Combined data + expected JSON output for graphs or blocks
		// Build result 
		
		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType. );
	}
	
	protected ITableDataFrame getSourceFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		throw new IllegalArgumentException("Must define the source frame");
	}

}
