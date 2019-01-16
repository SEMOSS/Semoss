package prerna.poi.main;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class BaseDatabaseCreator {

	public static final String TIME_KEY = "ENGINE:TIME";
	public static final String TIME_URL = "http://semoss.org/ontologies/Concept/TimeStamp";
	
	private RDFFileSesameEngine baseEng;

	//open without connection
	public BaseDatabaseCreator(String owlFile) {
		baseEng = new RDFFileSesameEngine();
		baseEng.openDB(null);
		baseEng.setFileName(owlFile);
	}

	//open with connection
	public BaseDatabaseCreator(IEngine engine, String owlFile) {
		baseEng = ((AbstractEngine) engine).getBaseDataEngine();
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
		
		baseEng.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
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
		
		baseEng.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{cleanSub, cleanPred, objValue, concept});
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
				DateFormat dateFormat = getFormatter();
				Calendar cal = Calendar.getInstance();
				String cleanObj = dateFormat.format(cal.getTime());
				baseEng.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
			}
			this.baseEng.exportDB();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Error in writing OWL file");
		}
	}

	public String exportBaseEngAsString(boolean addTimeStamp) throws IOException {
		try {
			//adding a time-stamp to the OWL file
			StringWriter writer = new StringWriter();
			if(addTimeStamp) {
				deleteExisitngTimestamp();
				DateFormat dateFormat = getFormatter();
				Calendar cal = Calendar.getInstance();
				String cleanObj = dateFormat.format(cal.getTime());
				baseEng.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
			}
			this.baseEng.exportDB(writer);
			writer.flush();
			return writer.toString();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Error in writing base engine db as OWL file");
		}
	}
	
	private void deleteExisitngTimestamp() {
		String getAllTimestampQuery = "SELECT DISTINCT ?time ?val WHERE { "
				+ "BIND(<http://semoss.org/ontologies/Concept/TimeStamp> AS ?time)"
				+ "{?time <" + TIME_KEY + "> ?val} "
				+ "}";
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(baseEng, getAllTimestampQuery);
		List<String> currTimes = new Vector<String>();
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			Object[] rawRow = row.getRawValues();
			Object[] cleanRow = row.getValues();
			currTimes.add(rawRow[0] + "");
			currTimes.add(cleanRow[1] + "");
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
	 * Standardize the time formatter
	 * @return
	 */
	public static DateFormat getFormatter() {
		return new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
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
	 * 
	 */
	public void closeBaseEng() {
		this.baseEng.closeDB();
	}
	
}
