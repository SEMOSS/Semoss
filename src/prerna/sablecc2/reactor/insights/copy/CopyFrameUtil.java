package prerna.sablecc2.reactor.insights.copy;

import java.io.IOException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;

public class CopyFrameUtil {

	private static final String CLASS_NAME = CopyFrameUtil.class.getName();

	private CopyFrameUtil() {

	}

	/**
	 * Copy the frame into a new frame
	 * Will also account for R/Py to rename the variable since the insight copy will
	 * share the same space
	 * @param insightContext
	 * @param frameToCopy
	 * @param limit
	 * @return
	 * @throws IOException
	 */
	public static ITableDataFrame copyFrame(Insight insightContext, ITableDataFrame frameToCopy, int limit) throws IOException {
		String oldName = frameToCopy.getName();

		// one thing that is consistent across all frames
		OwlTemporalEngineMeta newMetadata = frameToCopy.getMetaData().copy();
		
		ITableDataFrame newFrame =  null;
		try {
			// we need to set the correct context for the pandas + data.table frames
			// we will also account for names to be new
			
			if(frameToCopy instanceof PandasFrame) {
				newFrame = new PandasFrame();
				// set the metadata
				newFrame.setMetaData(newMetadata);
				PandasFrame dt = (PandasFrame) newFrame;
				
				dt.setJep( insightContext.getPy() );
				String newName = oldName + "_COPY";
				if(limit > 0) {
					dt.runScript(newName + " = " + oldName + "[:" + limit + "].copy(deep=True)");
				} else {
					dt.runScript(newName + " = " + oldName + ".copy(deep=True)");
				}
				// also do the wrapper
				dt.setName(newName);
				// the wrapper name is auto generated when you set name
				String newWrapperName = dt.getWrapperName();
				dt.runScript(PandasSyntaxHelper.makeWrapper(newWrapperName, newName));
				dt.getMetaData().modifyVertexName(oldName, newName);
				
			} else if (frameToCopy instanceof RDataTable) {
				newFrame = new RDataTable( insightContext.getRJavaTranslator(CLASS_NAME) );
				// set the metadata
				newFrame.setMetaData(newMetadata);
				RDataTable dt = (RDataTable) newFrame;

				String newName = oldName + "_COPY";
				if(limit > 0) {
					dt.executeRScript(newName + "<- " + oldName + "[1," + limit + ", ]");
				} else {
					dt.executeRScript(newName + "<- " + oldName);
				}
				dt.setName(newName);
				dt.getMetaData().modifyVertexName(oldName, newName);
				
			} else if(frameToCopy instanceof NativeFrame) {
				newFrame = new NativeFrame();
				// set the name
				// add the query struct
				// no need to limit on native frame?
				newFrame.setName(oldName);
				((NativeFrame) newFrame).mergeQueryStruct( ((NativeFrame) frameToCopy).getQueryStruct() );
			}
			
			else {
				newFrame = (ITableDataFrame) Class.forName(frameToCopy.getClass().getName()).newInstance();
				newFrame.setName(oldName);
				// just do a query on the current frame
				SelectQueryStruct qs = newMetadata.getFlatTableQs();
				// add the limit
				qs.setLimit(limit);
				IRawSelectWrapper iterator = frameToCopy.query(qs);
				// just the existing insert logic
				IImporter importer = ImportFactory.getImporter(newFrame, qs, iterator);
				importer.insertData(newMetadata);
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return newFrame;
	}

}
