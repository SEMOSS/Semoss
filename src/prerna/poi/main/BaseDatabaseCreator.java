package prerna.poi.main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public class BaseDatabaseCreator {

	private static final Logger classLogger = LogManager.getLogger(BaseDatabaseCreator.class);

	public static final String TIME_KEY = "ENGINE:TIME";
	public static final String TIME_URL = "http://semoss.org/ontologies/Concept/TimeStamp";
	
	private RDFFileSesameEngine baseEng;
	private final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
	
	//open without connection
	public BaseDatabaseCreator(String engineId, String owlFile) throws Exception {
		baseEng = new RDFFileSesameEngine();
		baseEng.open(new Properties());
		baseEng.setFileName(owlFile);
		baseEng.setEngineId(engineId + "_" + Constants.OWL_ENGINE_SUFFIX);
	}

	//open with connection
	public BaseDatabaseCreator(IDatabaseEngine engine, String owlFile) {
		baseEng = engine.getBaseDataEngine();
		baseEng.setFileName(owlFile);
	}

	/**
	 * Adding information into the base engine
	 * Currently assumes we are only adding URIs (object is never a literal)
	 * @param triple 			The triple to load into the engine and into baseDataHash
	 */
	public void addToBaseEngine(Object[] triple) {
		String sub = (String) triple[0];
		String pred = (String) triple[1];
		// is this a URI or a literal?
		boolean concept = Boolean.valueOf((boolean) triple[3]);

		String cleanSub = Utility.cleanString(sub, false);
		String cleanPred = Utility.cleanString(pred, false);
		
		Object objValue = triple[2];
		// if it is a URI
		// gotta clean up the value
		if(concept) {
			objValue = Utility.cleanString(objValue.toString(), false);
		}
		
		baseEng.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
	}
	
	/**
	 * Adding information into the base engine
	 * Currently assumes we are only adding URIs (object is never a literal)
	 * @param triple 			The triple to load into the engine and into baseDataHash
	 */
	public void removeFromBaseEngine(Object[] triple) {
		String sub = (String) triple[0];
		String pred = (String) triple[1];
		String obj = (String) triple[2];
		boolean concept = Boolean.valueOf((boolean) triple[3]);

		String cleanSub = Utility.cleanString(sub, false);
		String cleanPred = Utility.cleanString(pred, false);

		Object objValue = triple[2];
		// if it is a URI
		// gotta clean up the value
		if(concept) {
			objValue = Utility.cleanString(objValue.toString(), false);
		}
		
		baseEng.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
	}
	
	// set this as separate pieces as well
	public void addToBaseEngine(String subject, String predicate, String object) {
		addToBaseEngine(new Object[]{subject, predicate, object, true});
	}
	
	public void addToBaseEngine(String subject, String predicate, Object object, boolean isUri) {
		addToBaseEngine(new Object[]{subject, predicate, object, isUri});
	}
	
	// set this as separate pieces as well
	public void removeFromBaseEngine(String subject, String predicate, String object) {
		removeFromBaseEngine(new Object[]{subject, predicate, object, true});
	}

	public void removeFromBaseEngine(String subject, String predicate, Object object, boolean isUri) {
		removeFromBaseEngine(new Object[]{subject, predicate, object, isUri});
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void exportBaseEng(boolean addTimeStamp) throws IOException {
		try {
			//adding a time-stamp to the OWL file
			if(addTimeStamp) {
				deleteExisitngTimestamp();
				Calendar cal = Calendar.getInstance();
				String cleanObj = DATE_FORMATTER.format(cal.getTime());
				baseEng.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
			}
			this.baseEng.exportDB();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IOException("Error in writing OWL file");
		}
	}

	private void deleteExisitngTimestamp() {
		String getAllTimestampQuery = "SELECT DISTINCT ?time ?val WHERE { "
				+ "BIND(<http://semoss.org/ontologies/Concept/TimeStamp> AS ?time)"
				+ "{?time <" + TIME_KEY + "> ?val} "
				+ "}";
		
		List<String> currTimes = new Vector<String>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(baseEng, getAllTimestampQuery);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] rawRow = row.getRawValues();
				Object[] cleanRow = row.getValues();
				currTimes.add(rawRow[0] + "");
				currTimes.add(cleanRow[1] + "");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		for(int delIndex = 0; delIndex < currTimes.size(); delIndex+=2) {
			Object[] delTriples = new Object[4];
			delTriples[0] = currTimes.get(delIndex);
			delTriples[1] = TIME_KEY;
			delTriples[2] = currTimes.get(delIndex+1);
			delTriples[3] = false;
			
			this.baseEng.doAction(ACTION_TYPE.REMOVE_STATEMENT, delTriples);
		}
	}

	/**
	 * Commits the triples added to the base engine
	 */
	public void commit() {
		baseEng.commit();
	}
	
	/**
	 * 
	 * @return
	 */
	public RDFFileSesameEngine getBaseEng() {
		return this.baseEng;
	}

	/**
	 * @throws IOException 
	 * 
	 */
	public void closeBaseEng() throws IOException {
		this.baseEng.close();
	}
	
}
