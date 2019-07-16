package prerna.sablecc2.reactor.frame;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.ds.rdbms.sqlite.SQLiteFrame;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.RdbmsTypeEnum;

public class FrameFactory {

	private static final String CLASS_NAME = FrameFactory.class.getName();
	// this is so we only grab from DIHelper once
	private static boolean INIT = false;
	// grab default frame
	private static String DEFAULT_FRAME_TYPE = null;
	private static RdbmsTypeEnum RDBMS_TYPE = null;

	public static ITableDataFrame getFrame(Insight insight, String frameType, String alias) throws Exception {
		frameType = frameType.toUpperCase();
		
		Logger logger = null;
		if(!INIT) {
			init();
		}
		
		if(frameType.equals("DEFAULT")) {
			frameType = DEFAULT_FRAME_TYPE;
		}
		switch (frameType) {
			case "GRID": { 
				return getGrid(alias);
			}
			case "H2": { 
				return getGrid(alias);
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
				return new SQLiteFrame(alias); 
			}
		
		}
	}
	
	private static ITableDataFrame getGrid(String alias) throws Exception {
		if(RDBMS_TYPE == RdbmsTypeEnum.SQLITE) {
			return new SQLiteFrame(alias);
		}
		
		return new H2Frame(alias);
	}
	
	/**
	 * Init the frame factory configuration
	 */
	private static void init() {
		DEFAULT_FRAME_TYPE = DIHelper.getInstance().getProperty(Constants.DEFAULT_FRAME_TYPE);
		String defaultGridType = DIHelper.getInstance().getProperty(Constants.DEFAULT_GRID_TYPE);
		if(defaultGridType == null || defaultGridType.isEmpty()) {
			defaultGridType = "H2_DB";
		}
		
		RDBMS_TYPE = RdbmsTypeEnum.valueOf(defaultGridType);
		
		if(DEFAULT_FRAME_TYPE == null) {
			DEFAULT_FRAME_TYPE = "GRID";
		}
		INIT = true;
	}

	public static String getFrameType(ITableDataFrame frame) {
		if(frame instanceof AbstractRdbmsFrame) {
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
