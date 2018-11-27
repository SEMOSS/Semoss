package prerna.sablecc2.reactor.frame;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;

public class FrameFactory {

	private static final String CLASS_NAME = FrameFactory.class.getName();
	
	public static ITableDataFrame getFrame(Insight insight, String frameType, String alias) {
		Logger logger = null;
		switch (frameType.toUpperCase()) {
		case "GRID": { return new H2Frame(alias); }
		
		case "GRAPH": { return new TinkerFrame(); } 
		
		case "RFRAME": { 
			logger = LogManager.getLogger(CLASS_NAME);
			return new RDataTable(insight.getRJavaTranslator(logger), alias); 
		}
		case "R": { 
			logger = LogManager.getLogger(CLASS_NAME);
			return new RDataTable(insight.getRJavaTranslator(logger), alias); 
		}
		
		case "PYFRAME": {
			PandasFrame frame = new PandasFrame(alias);
			frame.setJep(insight.getPy());		
		}
		case "PANDAS": {
			PandasFrame frame = new PandasFrame(alias);
			frame.setJep(insight.getPy());		
		}
		case "PY": {
			PandasFrame frame = new PandasFrame(alias);
			frame.setJep(insight.getPy());		
		}
		
		case "NATIVE": { return new NativeFrame(); }
		
		default: { return new H2Frame(alias); }
		}
	}
	
	public static String getFrameType(ITableDataFrame frame) {
		if(frame instanceof H2Frame) {
			return "GRID";
		} else if(frame instanceof TinkerFrame) {
			return "GRAPH";
		} else if(frame instanceof RDataTable) {
			return "R";
		} else if(frame instanceof PandasFrame) {
			return "PY";
		} else if(frame instanceof NativeFrame) {
			return "NATIVE";
		} else {
			return null;
		}
	}
}
