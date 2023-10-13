package prerna.reactor.frame;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PyTranslator;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.ds.rdbms.postgres.PostgresFrame;
import prerna.ds.rdbms.sqlite.SQLiteFrame;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.RdbmsTypeEnum;

public class FrameFactory {

	private static final Logger logger = LogManager.getLogger(FrameFactory.class);
	
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
				TinkerFrame frame = new TinkerFrame(alias);
				return frame;
			} 
			case "TINKER": { 
				TinkerFrame frame = new TinkerFrame(alias);
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
				PyTranslator pyt = insight.getPyTranslator();
				pyt.setLogger(logger);
				frame.setTranslator(pyt);
				return frame;
			}
			case "PY": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				PyTranslator pyt = insight.getPyTranslator();
				pyt.setLogger(logger);
				frame.setTranslator(pyt);
				return frame;
			}
			case "PYFRAME": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				PyTranslator pyt = insight.getPyTranslator();
				pyt.setLogger(logger);
				frame.setTranslator(pyt);

				return frame;
			}
			case "PANDAS": {
				PandasFrame frame = new PandasFrame(alias);
				frame.setJep(insight.getPy());
				PyTranslator pyt = insight.getPyTranslator();
				pyt.setLogger(logger);
				frame.setTranslator(pyt);
				return frame;
			}
			
			case "NATIVE": { 
				NativeFrame frame = new NativeFrame(alias);
				return frame;
			}
			case "ENGINE": { 
				NativeFrame frame = new NativeFrame(alias);
				return frame;
			}
			
			default: { 
				return getGrid(alias); 
			}
		
		}
	}
	
	private static ITableDataFrame getGrid(String alias) throws Exception {
		if(RDBMS_TYPE == RdbmsTypeEnum.SQLITE) {
			return new SQLiteFrame(alias);
		} else if(RDBMS_TYPE == RdbmsTypeEnum.POSTGRES) {
			return new PostgresFrame(alias);
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
		try {
			RDBMS_TYPE = RdbmsTypeEnum.valueOf(defaultGridType);
		} catch(Exception e) {
			logger.error("Error occurred trying to set the default grid type for the application. Defaulting to h2");
			logger.error(Constants.STACKTRACE, e);
			defaultGridType = "H2_DB";
			RDBMS_TYPE = RdbmsTypeEnum.H2_DB;
		}
		
		if(DEFAULT_FRAME_TYPE == null) {
			DEFAULT_FRAME_TYPE = "GRID";
		}
		INIT = true;
	}

	public static boolean canCacheFrameQueries(ITableDataFrame frame) {
		if(frame == null) {
			return false;
		}
		boolean cache = false;
		if(frame instanceof RDataTable) {
			cache = true;
		} else if(frame instanceof PandasFrame) {
			cache = true;
		} else if(frame instanceof NativeFrame) {
			cache = ((NativeFrame) frame).engineQueryCacheable();
		}
		
		return cache;
	}
}
