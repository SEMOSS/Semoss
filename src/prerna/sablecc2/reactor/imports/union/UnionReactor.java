package prerna.sablecc2.reactor.imports.union;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;


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
	
	private final static String UNION_COL_MAPPING = "cols";
	private final static String UNION_TYPE = "unionType";
	private final static String UNION_FRAME_A = "From";
	private final static String UNION_FRAME_B = "To";
	
	public UnionReactor() {
		this.keysToGet = new String[] {UNION_COL_MAPPING, UNION_TYPE, UNION_FRAME_A, UNION_FRAME_B};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		ITableDataFrame frame = getFrame();
		Logger logger = getLogger(frame.getClass().getName());
		frame.setLogger(logger);
		ITableDataFrame curFrame = this.insight.getCurFrame();
		String unionType = this.keyValue.get(this.keysToGet[3]);
		//Get the specific routine based on the frame.
		UnionRoutine routine = UnionFactory.getUnionRoutine(frame);
		ITableDataFrame unionFrame = null;
		////"(Historically_Black, Historically_Black);(Men_Only, Men_Only),(School, School),(SCHOOLS_UNIQUE_ROW_ID, SCHOOLS_UNIQUE_ROW_ID),(Women_Only, Women_Only)",
		if(this.keyValue.get(this.keysToGet[0]) == null || this.keyValue.get(this.keysToGet[0]).isEmpty())
			throw new IllegalArgumentException("There are no columns selected. Please add columns for union.");
		Map<String, String> colMaps = new HashMap<>();
		String[] colMappings = this.keyValue.get(this.keysToGet[0]).split(";");
		for(String colMap : colMappings) {
			String[] cols = colMap.split(",");
			String rightCol = cols[0].replace("(", "").trim();
			String leftCol = cols[1].replace(")", "").trim();
			colMaps.put(leftCol, rightCol);
		}
		try {
			//Set the cols that needs to be mapped
			routine.setColMapping(colMaps);
			//Run the routine here.
			unionFrame = routine.performUnion(frame, curFrame, unionType, this.insight, logger);
		} catch (Exception e) {
			throw new SemossPixelException(e.getMessage());
		}
		unionFrame.syncHeaders();	
		unionFrame.clearCachedMetrics();
		unionFrame.clearQueryCache();

		NounMetadata noun = new NounMetadata(unionFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		
		if(unionFrame != frame && !(unionFrame instanceof PandasFrame)) {
			if(frame.getName() != null) {
				this.insight.getVarStore().put(frame.getName(), noun);
			} 
			if(unionFrame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
				this.insight.setDataMaker(unionFrame);
			}
		}else {
			this.insight.getVarStore().put(unionFrame.getName(), noun);
			this.insight.setDataMaker(unionFrame);
		}
		
		return noun;
	}
	
	/**
	 * Below method queries the store and gets the appropriate
	 * default frame.
	 *  
	 */
	
	protected ITableDataFrame getFrame() {
		// try specific key
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[2]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
		return defaultFrame;
	}
	
}
