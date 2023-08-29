package prerna.engine.impl.model.inferencetracking;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang3.tuple.Pair;

import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsOwlCreation {
	
	// each column name paired to its type in a var
	private List<Pair<String, String>> agentColumns = null;
	private List<Pair<String, String>> roomColumns = null;
	private List<Pair<String, String>> messageColumns = null;
	private List<Pair<String, String>> feedbackColumns = null;

	// pairs table name with table's primary keys 
	private List<Pair<String, Pair<List<String>, List<String>>>> primaryKeys = null;
	
	// pairs table name with table's foreign keys 
	private List<Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>>> foreignKeys = null;
	// Pairs table name with its respective columns
	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;
	
	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new Vector<String>();
	static {
		conceptsRequired.add("AGENT");
		conceptsRequired.add("ROOM");
		conceptsRequired.add("MESSAGE");
	}
	
	private IRDBMSEngine modelInferenceDb;
	
	
	public ModelInferenceLogsOwlCreation(IRDBMSEngine modelInferenceDb) {
		this.modelInferenceDb = modelInferenceDb;
		createColumnsAndTypes(this.modelInferenceDb.getQueryUtil());
		definePrimaryKeys();
		defineForeignKeys(); // TODO make this generic for all rdms engines
	}
	
	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String DOUBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();
		this.agentColumns = Arrays.asList(
				Pair.of("AGENT_ID", "VARCHAR(50)"),
				Pair.of("AGENT_NAME", "VARCHAR(255)"),
				Pair.of("DESCRIPTION", "VARCHAR(255)"),
				Pair.of("AGENT_TYPE", "VARCHAR(50)"),
				Pair.of("AUTHOR", "VARCHAR(255)"),
				Pair.of("DATE_CREATED", TIMESTAMP_DATATYPE_NAME)
			);
		
		this.roomColumns = Arrays.asList(
				Pair.of("INSIGHT_ID", "VARCHAR(50)"),
				Pair.of("ROOM_NAME", "VARCHAR(255)"),
				Pair.of("ROOM_CONTEXT", "VARCHAR(255)"),
				//Pair.of("ROOM_CONFIG_DATA", CLOB_DATATYPE_NAME),
				Pair.of("USER_ID", "VARCHAR(255)"),
				Pair.of("AGENT_TYPE", "VARCHAR(50)"),
				Pair.of("IS_ACTIVE",BOOLEAN_DATATYPE_NAME),
				Pair.of("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.of("PROJECT_ID", "VARCHAR(50)"),
				Pair.of("PROJECT_NAME", "VARCHAR(255)"),
				Pair.of("AGENT_ID", "VARCHAR(50)")
			);
		
		this.messageColumns = Arrays.asList(
				Pair.of("MESSAGE_ID", "VARCHAR(50)"),
				Pair.of("MESSAGE_TYPE", "VARCHAR(50)"),
				Pair.of("MESSAGE_DATA", CLOB_DATATYPE_NAME),
				Pair.of("MESSAGE_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.of("MESSAGE_METHOD", "VARCHAR(50)"),
				Pair.of("RESPONSE_TIME", DOUBLE_DATATYPE_NAME),
				Pair.of("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.of("AGENT_ID", "VARCHAR(50)"),
				Pair.of("INSIGHT_ID", "VARCHAR(50)"),
				Pair.of("SESSIONID", "VARCHAR(255)"),
				Pair.of("USER_ID", "VARCHAR(255)")
			);
		
		this.feedbackColumns = Arrays.asList(
				Pair.of("MESSAGE_ID", "VARCHAR(50)"),
				Pair.of("MESSAGE_TYPE", "VARCHAR(50)"),
				Pair.of("FEEDBACK_TEXT", "VARCHAR(MAX)"),
				Pair.of("FEEDBACK_DATE", TIMESTAMP_DATATYPE_NAME),
				Pair.of("RATING", BOOLEAN_DATATYPE_NAME)
			);
		
		this.allSchemas = Arrays.asList(
				Pair.of("AGENT", agentColumns),
				Pair.of("ROOM", roomColumns),
				Pair.of("MESSAGE", messageColumns),
				Pair.of("FEEDBACK", feedbackColumns)
			);
	}
	
	public void definePrimaryKeys() {
		// returns ArrayList so its ordered
		this.primaryKeys = Arrays.asList(
				Pair.of("AGENT", Pair.of(Arrays.asList("AGENT_ID"), Arrays.asList("VARCHAR(50)"))),
				Pair.of("ROOM", Pair.of(Arrays.asList("INSIGHT_ID"), Arrays.asList("VARCHAR(50)"))),
				Pair.of("MESSAGE", Pair.of(Arrays.asList("MESSAGE_ID","MESSAGE_TYPE"), Arrays.asList("VARCHAR(50)","VARCHAR(50)")))
			);
	}
	
	public void defineForeignKeys() {
		this.foreignKeys = Arrays.asList(
				Pair.of("ROOM", Pair.of(Arrays.asList("AGENT_ID"), Pair.of(Arrays.asList("AGENT"), Arrays.asList("AGENT_ID")))),
				Pair.of("MESSAGE", Pair.of(Arrays.asList("INSIGHT_ID","AGENT_ID"), Pair.of(Arrays.asList("ROOM","AGENT"), Arrays.asList("INSIGHT_ID","AGENT_ID")))),
				Pair.of("FEEDBACK", Pair.of(Arrays.asList("MESSAGE_ID,MESSAGE_TYPE"), Pair.of(Arrays.asList("MESSAGE"), Arrays.asList("MESSAGE_ID,MESSAGE_TYPE"))))
			);
	}
	
	/**
	 * Determine if we need to remake the OWL
	 * @return
	 */
	public boolean needsRemake() {
		/*
		 * This is a very simple check
		 * Just looking at the tables
		 * Not doing anything with columns but should eventually do that
		 */
		
		List<String> cleanConcepts = new Vector<String>();
		List<String> concepts = modelInferenceDb.getPhysicalConcepts();
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}
		
		boolean check1 = cleanConcepts.containsAll(conceptsRequired);
		
		if (!check1) {
			return true;
		}
		
		// check all columns
		for (Pair<String, List<Pair<String, String>>> tableWithColumns : allSchemas) {
			String tableName = tableWithColumns.getLeft();
			String[] columnNames = tableWithColumns.getRight().stream()
					.map(Pair::getLeft).toArray(String[]::new);

			for (String columnName : columnNames) {
				if (columnChecks(tableName, columnName)) {
					return true;
				}
			}
		}
		
		// does not need to be remade
		return false;
	}
	
	
	private boolean columnChecks(String tableName, String columnName) {
		String propsURI = "http://semoss.org/ontologies/Concept/" + tableName;
		String relationURI = "http://semoss.org/ontologies/Relation/Contains/" 
				+ columnName + "/" + tableName; 

		List<String> props = modelInferenceDb.getPropertyUris4PhysicalUri(propsURI);	
		if(!props.contains(relationURI)) {
			return true;
		}
		
		return false;
	}
		
	
	/**
	 * Remake the OWL 
	 * @throws Exception 
	 */
	public void remakeOwl() throws Exception {
		// get the existing engine and close it
		RDFFileSesameEngine baseEngine = modelInferenceDb.getBaseDataEngine();
		if(baseEngine != null) {
			baseEngine.close();
		}
		
		// now delete the file if exists
		// and we will make a new
		String owlLocation = modelInferenceDb.getOWL();
		
		File f = new File(owlLocation);
		if(f.exists()) {
			f.delete();
		}
		
		// write the new OWL
		writeNewOwl(owlLocation);
		
		// now reload into security db
		modelInferenceDb.setOWL(owlLocation);
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws Exception 
	 */
	private void writeNewOwl(String owlLocation) throws Exception {
		Owler owler = new Owler(owlLocation, DATABASE_TYPE.RDBMS);

		// ENGINE	
		for (Pair<String, List<Pair<String, String>>> columns : allSchemas) {
			String tableName = columns.getLeft();
			owler.addConcept(tableName, null, null);
			for (Pair<String, String> x : columns.getRight()) {	
				owler.addProp(tableName, x.getLeft(), x.getRight());
			}
		}
		
		owler.commit();
		owler.export();
	}
	
	public List<Pair<String, List<Pair<String, String>>>> getDBSchema() {
		return this.allSchemas;
	}
	
	public List<Pair<String, Pair<List<String>, List<String>>>> getDBPrimaryKeys() {
		return this.primaryKeys;
	}
	
	public List<Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>>> getDBForeignKeys() {
		return this.foreignKeys;
	}
	
}
