package prerna.ds.export.graph;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;

public class GraphExporterFactory {

	/**
	 * Based on the frame, get the correct exporter
	 * @param frame
	 * @return
	 */
	static IGraphExporter getExporter(ITableDataFrame frame) {
		IGraphExporter graphExporter = null;
		if(frame instanceof TinkerFrame) {
			graphExporter = new TinkerFrameGraphExporter((TinkerFrame) frame);
		} else if(frame instanceof H2Frame) {
			graphExporter = new RdbmsGraphExporter((H2Frame) frame);
		} else if(frame instanceof RDataTable) {
			
		} else if(frame instanceof NativeFrame) {
			
		}
		
		return graphExporter;
	}
	
	
}
