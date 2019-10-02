package prerna.sablecc2.reactor.frame;

import java.util.Set;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;

public class PurgeReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = PurgeReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		// get the frame
		ITableDataFrame frame = getFrame();
		GenRowFilters curFilters = frame.getFrameFilters().copy();
		SelectQueryStruct qs = frame.getMetaData().getFlatTableQs();
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
			interp.setAdditionalTypes(dt.getMetaData().getHeaderToAdtlTypeMap());
			interp.setLogger(logger);
			logger.info("Generating filter R Data Table query...");
			String query = interp.composeQuery();
			logger.info("Done generating filter R Data Table query");
			
			// execute
			dt.executeRScript(frame.getName() + "<- {" + query + "};");
			
			// assign newFrame back to frame
			newFrame = frame;
			newFrame.getFrameFilters().removeAllFilters();
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
//		} 
		else {
			logger.info("Running generic purge logic");

			// go through generic logic
			IRawSelectWrapper it = frame.query(qs);
			
			// new frame
			String frameType = FrameFactory.getFrameType(frame);
			try {
				newFrame = FrameFactory.getFrame(this.insight, frameType, null);
			} catch (Exception e) {
				throw new IllegalArgumentException("Error occured trying to create frame of type " + frameType, e);
			}
			// insert the data for the new frame
			IImporter importer = ImportFactory.getImporter(newFrame, qs, it);
			importer.insertData();		
			
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

}
