package prerna.reactor.frame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PurgeReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = PurgeReactor.class.getName();
	
	public PurgeReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.FILTERS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		// get the frame
		ITableDataFrame frame = getFrame();
		GenRowFilters curFilters = getFilters();
		if(curFilters == null) {
			curFilters = frame.getFrameFilters().copy();
		}
		SelectQueryStruct qs = frame.getMetaData().getFlatTableQs(false);
		qs.setExplicitFilters(curFilters);
		qs.setFrame(frame);
		
		ITableDataFrame newFrame = null;
		
		// i am going to optimize here
		// so we can make things faster
		if(frame instanceof RDataTable) {
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			logger.info("Running optimized purge for R frame");
			RDataTable dt = (RDataTable) frame;

			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dt.getName());
			interp.setColDataTypes(dt.getMetaData().getHeaderToTypeMap());
//			interp.setAdditionalTypes(dt.getMetaData().getHeaderToAdtlTypeMap());
			interp.setLogger(logger);
			logger.info("Generating filter R Data Table query...");
			String query = interp.composeQuery();
			logger.info("Done generating filter R Data Table query");
			
			// execute
			dt.executeRScript(frame.getName() + "<- {" + query + "};");
			
			// assign newFrame back to frame
			newFrame = frame;
			newFrame.getFrameFilters().removeAllFilters();
			// clear the cached queries
			newFrame.clearCachedMetrics();
			newFrame.clearQueryCache();
		}
		//TODO: test this
		//TODO: test this
		//TODO: test this
		//TODO: test this
//		else if(frame instanceof PandasFrame) {
//			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
//			logger.info("Running optimized purge for Python frame");
//			PandasFrame dt = (PandasFrame) frame;
//
//			PandasInterpreter interp = new PandasInterpreter();
//			interp.setDataTableName(dt.getWrapperName() + ".cache['data']");
//			interp.setDataTypeMap(dt.getMetaData().getHeaderToTypeMap());
//			interp.setQueryStruct(qs);
//			logger.info("Generating filter Pandas query...");
//			String query = interp.composeQuery();
//			logger.info("Done generating filter Pandas query");
//			
//			// execute
//			dt.runScript(dt.getName() + " = " + query);
//			// TODO: unsure if i need to do the below
//			dt.runScript(PandasSyntaxHelper.makeWrapper(dt.getWrapperName(), dt.getName()));
//			
//			// assign newFrame back to frame
//			newFrame = frame;
//			newFrame.getFrameFilters().removeAllFilters();
			// clear the cached queries
//			newFrame.clearCachedMetrics();
//			newFrame.clearQueryCache();		
//		} 
		else {
			logger.info("Running generic purge logic");

			// new frame
			String frameType = frame.getFrameType().getTypeAsString();
			try {
				newFrame = FrameFactory.getFrame(this.insight, frameType, null);
				newFrame.setName(frame.getName());
			} catch (Exception e) {
				throw new IllegalArgumentException("Error occurred trying to create frame of type " + frameType, e);
			}
			// insert the data for the new frame
			// go through generic logic
			IRawSelectWrapper it;
			try {
				it = frame.query(qs);
				IImporter importer = ImportFactory.getImporter(newFrame, qs, it);
				importer.insertData();
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(e.getMessage());
			}

			NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
			// see if this is overriding any reference
			VarStore varStore = this.insight.getVarStore();
			// add new reference
			varStore.put(newFrame.getName(), noun);
			// override other references
			Set<String> curReferences = varStore.getAllAliasForObjectReference(frame);
			// switch to the new frame
			for(String reference : curReferences) {
				varStore.put(reference, noun);
			}
		}
		
		// return the noun
		return new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	/**
	 * Take in user defined filters
	 * @return
	 */
	protected GenRowFilters getFilters() {
		// generate a grf with the wanted filters
		GenRowFilters grf = new GenRowFilters();
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			SelectQueryStruct qs = (SelectQueryStruct) this.curRow.get(i);
			if (qs != null) {
				grf.merge(qs.getCombinedFilters());
			}
		}
		if(grf != null && !grf.isEmpty()) {
			return grf;
		}
		
		return null;
	}
	
	@Override
	public Map<String, List<Map>> getStoreMap() {
		Map<String, List<Map>> inputMap = super.getStoreMap();
		List<Map> list = inputMap.get(ReactorKeysEnum.FILTERS.getKey());
		List<Map> newList = new ArrayList<>();
		for(Map basicInput : list) {
			Object qsObj = basicInput.get("value");
			if(qsObj instanceof SelectQueryStruct) {
				SelectQueryStruct qs = (SelectQueryStruct) qsObj;
				
				Map<String, Object> newInput = new HashMap<>();
				newInput.put("type", PixelDataType.FILTER.getKey());
				GenRowFilters combinedFilters = qs.getCombinedFilters();
				newInput.put("value", combinedFilters.getFormatedFilters());
				newList.add(newInput);
			}
		}
		
		inputMap.put(ReactorKeysEnum.FILTERS.getKey(), newList);
		inputMap.put(ReactorKeysEnum.QUERY_STRUCT.getKey(), list);
		return inputMap;
	}

}
