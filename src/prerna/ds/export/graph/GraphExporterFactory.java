package prerna.ds.export.graph;

import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class GraphExporterFactory {

	private GraphExporterFactory() {
		
	}
	
	/**
	 * Based on the frame, get the correct exporter
	 * @param frame
	 * @return
	 */
	public static IGraphExporter getExporter(IDataMaker frame) {
		IGraphExporter graphExporter = null;
		if(frame instanceof TinkerFrame) {
			graphExporter = new TinkerFrameGraphExporter((TinkerFrame) frame);
		} else if(frame instanceof H2Frame) {
			if( ((H2Frame) frame).hasPrimKey()) {
				graphExporter = new FlatRdbmsGraphExporter((H2Frame) frame);
			} else {
				graphExporter = new RdbmsGraphExporter((H2Frame) frame);
			}
		} else if(frame instanceof RDataTable) {
			graphExporter = new RGraphExporter((RDataTable) frame);
		} else if(frame instanceof NativeFrame) {
			
		}
		
		return graphExporter;
	}
	
	
}
