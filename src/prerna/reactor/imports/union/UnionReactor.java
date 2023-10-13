package prerna.reactor.imports.union;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.om.Insight;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * This reactor is the entry point for the Union functionality.
 * It extends the AbstractFrameReactor, hence it overrides the
 * execute method. In the execute method, it takes the two frames
 * from from the pipeline and then based on the frame types it gets
 * the specific Py\R routines. 
 * 
 * Note - As of now only Py and R routines has been implemented 
 * because of upcoming Nov release and later its going to get expanded
 * to Grid and Native.
 *
 */
public class UnionReactor extends AbstractFrameReactor{
	
	private static final String CLASS_NAME = UnionReactor.class.getName();

	private final static String FRAME1 = "frame1";
	private final static String FRAME2 = "frame2";
	private final static String UNION_TYPE = "unionType";
	private final static String UNION_COL_MAPPING = "mapping";
	
	public UnionReactor() {
		this.keysToGet = new String[] {FRAME1, FRAME2, UNION_TYPE, UNION_COL_MAPPING};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		organizeKeys();
		ITableDataFrame frame1 = getFrame(FRAME1);
		ITableDataFrame frame2 = getFrame(FRAME2);
		String unionType = this.keyValue.get(this.keysToGet[2]);
		Map<String, String> colMaps = getColMapping();
		if(colMaps == null) {
			// assume we are mapping everything and headers are the same
			colMaps = new HashMap<>();
			String[] headers = frame1.getColumnHeaders();
			for(String head : headers) {
				colMaps.put(head, head);
			}
		}
		// get the routine object
		UnionRoutine routine = UnionFactory.getUnionRoutine(frame1);
		ITableDataFrame unionFrame = null;
		try {
			//Set the cols that needs to be mapped
			routine.setColMapping(colMaps);
			//Run the routine here.
			unionFrame = routine.performUnion(frame1, frame2, unionType, this.insight, logger);
		} catch (Exception e) {
			throw new SemossPixelException(e.getMessage());
		}
		unionFrame.syncHeaders();	
		unionFrame.clearCachedMetrics();
		unionFrame.clearQueryCache();

		NounMetadata noun = new NounMetadata(unionFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		if(unionFrame != frame1 && !(unionFrame instanceof PandasFrame)) {
			if(frame1.getName() != null) {
				this.insight.getVarStore().put(frame1.getName(), noun);
			} 
			if(unionFrame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
				this.insight.setDataMaker(unionFrame);
			}
		} else {
			this.insight.getVarStore().put(unionFrame.getName(), noun);
			this.insight.setDataMaker(unionFrame);
		}
		
		return noun;
	}
	
	/**
	 * Get the frame
	 * @return
	 */
	private ITableDataFrame getFrame(String key) {
		GenRowStruct frameGrs = this.store.getNoun(key);
		if(frameGrs == null || frameGrs.isEmpty()) {
			throw new IllegalArgumentException("Must define " + key);
		}
		return (ITableDataFrame) frameGrs.get(0);
	}
	
	/**
	 * Get the column mapping
	 * @return
	 */
	private Map<String, String> getColMapping() {
		// try specific key
		GenRowStruct mappingGrs = this.store.getNoun(UNION_COL_MAPPING);
		if(mappingGrs == null || mappingGrs.isEmpty()) {
			return null;
		}
		return (Map<String, String>) mappingGrs.get(0);
	}
	
	@Override
	public String getReactorDescription() {
		return super.getReactorDescription();
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(FRAME1)) {
			return "This is the first frame where the data will be unioned unto";
		} else if(key.equals(FRAME2)) {
			return "This is the second frame where the data will be queried to union unto the first frame";
		} else if(key.equals(UNION_TYPE)) {
			return "This is either \"union\" or \"union_all\"";
		} else if(key.equals(UNION_COL_MAPPING)) {
			return "This is a map {\"frame1Header\":\"frame2Header\"}. If empty, assumes all headers match between the two frames.";
		}
		return super.getDescriptionForKey(key);
	}
	
}
