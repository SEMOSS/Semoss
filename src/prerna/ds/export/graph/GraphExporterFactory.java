package prerna.ds.export.graph;

import java.awt.Color;
import java.util.Map;
import prerna.ds.TinkerFrame;
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
		} 
//		else if(frame instanceof H2Frame) {
//			graphExporter = new RdbmsGraphExporter((H2Frame) frame);
//		} else if(frame instanceof RDataTable) {
//			graphExporter = new RGraphExporter((RDataTable) frame);
//		} 
		
		return graphExporter;
	}
	
	public static IGraphExporter getExporter(IDataMaker frame, Map<String, Color> colorMap) {
		IGraphExporter graphExporter = null;
		if(frame instanceof TinkerFrame) {
			graphExporter = new TinkerFrameGraphExporter((TinkerFrame) frame, colorMap);
		} 
		return graphExporter;
	}
	
	
}
