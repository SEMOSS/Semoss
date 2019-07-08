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
import prerna.util.Constants;
import prerna.util.DIHelper;

public class FrameFactory {

	private static final String CLASS_NAME = FrameFactory.class.getName();
	// this is so we only grab from DIHelper once
	private static boolean INIT = false;
	// grab default frame
	private static String DEFAULT_FRAME_TYPE = null;
	
	public static ITableDataFrame getFrame(Insight insight, String frameType, String alias) {
		Logger logger = null;
		if(!INIT) {
			init();
		}
		
		if(frameType.toUpperCase().equals("DEFAULT")) {
			frameType = DEFAULT_FRAME_TYPE;
		}
		switch (frameType.toUpperCase()) {
			case "GRID": { 
				return new H2Frame(alias); 
			}
			case "H2": { 
				return new H2Frame(alias); 
			}
			
			case "GRAPH": { 
				TinkerFrame frame = new TinkerFrame();
				frame.setName(alias);
				return frame;
			} 
			case "TINKER": { 
				TinkerFrame frame = new TinkerFrame();
				frame.setName(alias);
				return frame;
			} 
			
			case "R": { 
				logger = LogManager.getLogger(CLASS_NAME);
				return new RDataTable(insight.getRJavaTranslator(logger), alias); 
			}
			case "RFRAME": { 
				logger = LogManager.getLogger(CLASS_NAME);
				return new RDataTable(insight.getRJavaTranslator(logger), alias); 
			}
			case "DATATABLE": { 
				logger = LogManager.getLogger(CLASS_NAME);
				return new RDataTable(insight.getRJavaTranslator(logger), alias); 
			}
			
			case "PYTHON": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				return frame;
			}
			case "PY": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				return frame;
			}
			case "PYFRAME": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				return frame;
			}
			case "PANDAS": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				return frame;
			}
			
			case "NATIVE": { 
				NativeFrame frame = new NativeFrame();
				frame.setName(alias);
				return frame;
			}
			case "ENGINE": { 
				NativeFrame frame = new NativeFrame();
				frame.setName(alias);
				return frame;
			}
			
			default: { 
				return new H2Frame(alias); 
			}
		
		}
	}
	
	private static void init() {
		DEFAULT_FRAME_TYPE =  DIHelper.getInstance().getProperty(Constants.DEFAULT_FRAME_TYPE);
		INIT = true;
		
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
