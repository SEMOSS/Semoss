package prerna.poi.main;

import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Map;

import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;

public class BaseDatabaseCreator {

	public static final String TIME_KEY = "ENGINE:TIME";
	public static final String TIME_URL = "http://semoss.org/ontologies/Concept/TimeStamp";
	
	private RDFFileSesameEngine baseEng;
	protected Hashtable<String, String> baseDataHash = new Hashtable<String, String>();

	//open without connection
	public BaseDatabaseCreator(String owlFile) {
		baseEng = new RDFFileSesameEngine();
		baseEng.openDB(null);
		baseEng.setFileName(owlFile);
	}

	//open with connection
	@SuppressWarnings("unchecked")
	public BaseDatabaseCreator(IEngine engine, String owlFile) {
		baseEng = ((AbstractEngine) engine).getBaseDataEngine();
		baseDataHash = ((AbstractEngine) engine).getBaseHash();
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
		String obj = (String) triple[2];
		boolean concept = Boolean.valueOf((boolean) triple[3]);

		String cleanSub = Utility.cleanString(sub, false);
		String cleanPred = Utility.cleanString(pred, false);
		String cleanObj = Utility.cleanString(obj, false);

		baseEng.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{cleanSub, cleanPred, cleanObj, concept});
		baseDataHash.put(cleanSub, cleanSub);
		baseDataHash.put(cleanPred, cleanPred);
		baseDataHash.put(cleanObj, cleanObj);
	}
	
	// set this as separate pieces as well
	public void addToBaseEngine(String subject, String predicate, String object)
	{
		addToBaseEngine(new Object[]{subject, predicate, object, true});
	}

	/**
	 * 
	 * @throws IOException
	 */
	public void exportBaseEng(boolean addTimeStamp) throws IOException {
		try {
			//adding a time-stamp to the OWL file
			if(addTimeStamp) {
				DateFormat dateFormat = getFormatter();
				Calendar cal = Calendar.getInstance();
				String cleanObj = dateFormat.format(cal.getTime());
				baseEng.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
			}
			this.baseEng.exportDB();
		} catch (RepositoryException | RDFHandlerException | IOException e) {
			e.printStackTrace();
			throw new IOException("Error in writing base engine db as OWL file");
		}
	}

	public String exportBaseEngAsString(boolean addTimeStamp) throws IOException {
		try {
			//adding a time-stamp to the OWL file
			StringWriter writer = new StringWriter();
			if(addTimeStamp) {
				DateFormat dateFormat = getFormatter();
				Calendar cal = Calendar.getInstance();
				String cleanObj = dateFormat.format(cal.getTime());
				baseEng.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{TIME_URL, TIME_KEY, cleanObj, false});
			}
			this.baseEng.exportDB(writer);
			writer.flush();
			return writer.toString();
		} catch (RepositoryException | RDFHandlerException | IOException e) {
			e.printStackTrace();
			throw new IOException("Error in writing base engine db as OWL file");
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
	 */
	public Map<String, String> getBaseDataHash() {
		return this.baseDataHash;
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
	
	public void removeFromBaseEngine(Object[] triple){
		this.baseEng.removeStatement(triple);
	}
}
