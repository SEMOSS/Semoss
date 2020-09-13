package prerna.om;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class MapDb {

	private static final Logger LOGGER = LogManager.getLogger(MapDb.class.getName());

	private static MapDb singleton;
	private DB db;
	private ConcurrentNavigableMap<String, Insight> insightMap;
	
	private MapDb() {
		String path = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		File f = new File(path + "/insightMapDb");
		db = DBMaker.fileDB(f)
		        .closeOnJvmShutdown()
		        .make();
		LOGGER.info("Generated DB object for storage...");
		this.insightMap = db.treeMap("insightMap");
	}
	
	public static MapDb getInstance() {
		if(singleton == null) {
			singleton = new MapDb();
		}
		return singleton;
	}
	
	public void commit() {
		db.commit();
		LOGGER.info("Committed DB object to file...");
	}
	
	public void storeValue(String insightId, Insight in) {
		insightMap.put(insightId, in);
		LOGGER.info("Stored new Insight object with id = "  + insightId + "...");
	}
	
	public Insight getValue(String insightId) {
		LOGGER.info("Retrieving Insight object with id = "  + insightId + "...");
		Insight in = insightMap.get(insightId);
		return in;
	}
	
	public boolean containsKey(String insightId) {
		return insightMap.containsKey(insightId);
	}
	
	
}
