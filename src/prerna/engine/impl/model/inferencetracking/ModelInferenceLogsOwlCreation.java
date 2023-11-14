package prerna.engine.impl.model.inferencetracking;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
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
		conceptsRequired.add("FEEDBACK");
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
				Pair.with("AGENT_ID", "VARCHAR(50)"),
				Pair.with("AGENT_NAME", "VARCHAR(255)"),
				Pair.with("DESCRIPTION", "VARCHAR(255)"),
				Pair.with("AGENT_TYPE", "VARCHAR(50)"),
				Pair.with("AUTHOR", "VARCHAR(255)"),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME)
			);
		
		this.roomColumns = Arrays.asList(
				Pair.with("INSIGHT_ID", "VARCHAR(50)"),
				Pair.with("ROOM_NAME", "VARCHAR(255)"),
				Pair.with("ROOM_CONTEXT", CLOB_DATATYPE_NAME),
				//Pair.with("ROOM_CONFIG_DATA", CLOB_DATATYPE_NAME),
				Pair.with("USER_ID", "VARCHAR(255)"),
				Pair.with("AGENT_TYPE", "VARCHAR(50)"),
				Pair.with("IS_ACTIVE",BOOLEAN_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROJECT_ID", "VARCHAR(50)"),
				Pair.with("PROJECT_NAME", "VARCHAR(255)"),
				Pair.with("AGENT_ID", "VARCHAR(50)")
			);
		
		this.messageColumns = Arrays.asList(
				Pair.with("MESSAGE_ID", "VARCHAR(50)"),
				Pair.with("MESSAGE_TYPE", "VARCHAR(50)"),
				Pair.with("MESSAGE_DATA", CLOB_DATATYPE_NAME),
				Pair.with("MESSAGE_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("MESSAGE_METHOD", "VARCHAR(50)"),
				Pair.with("RESPONSE_TIME", DOUBLE_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("AGENT_ID", "VARCHAR(50)"),
				Pair.with("INSIGHT_ID", "VARCHAR(50)"),
				Pair.with("SESSIONID", "VARCHAR(255)"),
				Pair.with("USER_ID", "VARCHAR(255)")
			);
		
		this.feedbackColumns = Arrays.asList(
				Pair.with("MESSAGE_ID", "VARCHAR(50)"),
				Pair.with("MESSAGE_TYPE", "VARCHAR(50)"),
				Pair.with("FEEDBACK_TEXT", "VARCHAR(MAX)"),
				Pair.with("FEEDBACK_DATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RATING", BOOLEAN_DATATYPE_NAME)
			);
		
		this.allSchemas = Arrays.asList(
				Pair.with("AGENT", agentColumns),
				Pair.with("ROOM", roomColumns),
				Pair.with("MESSAGE", messageColumns),
				Pair.with("FEEDBACK", feedbackColumns)
			);
	}
	
	public void definePrimaryKeys() {
		// returns ArrayList so its ordered
		this.primaryKeys = Arrays.asList(
				Pair.with("AGENT", Pair.with(Arrays.asList("AGENT_ID"), Arrays.asList("VARCHAR(50)"))),
				Pair.with("ROOM", Pair.with(Arrays.asList("INSIGHT_ID"), Arrays.asList("VARCHAR(50)"))),
				Pair.with("MESSAGE", Pair.with(Arrays.asList("MESSAGE_ID","MESSAGE_TYPE"), Arrays.asList("VARCHAR(50)","VARCHAR(50)")))
			);
	}
	
	public void defineForeignKeys() {
		this.foreignKeys = Arrays.asList(
				Pair.with("ROOM", Pair.with(Arrays.asList("AGENT_ID"), Pair.with(Arrays.asList("AGENT"), Arrays.asList("AGENT_ID")))),
				Pair.with("MESSAGE", Pair.with(Arrays.asList("INSIGHT_ID","AGENT_ID"), Pair.with(Arrays.asList("ROOM","AGENT"), Arrays.asList("INSIGHT_ID","AGENT_ID")))),
				Pair.with("FEEDBACK", Pair.with(Arrays.asList("MESSAGE_ID,MESSAGE_TYPE"), Pair.with(Arrays.asList("MESSAGE"), Arrays.asList("MESSAGE_ID,MESSAGE_TYPE"))))
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
			String tableName = tableWithColumns.getValue0();
			String[] columnNames = tableWithColumns.getValue1().stream()
					.map(Pair::getValue0).toArray(String[]::new);

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
		try(WriteOWLEngine owlEngine = modelInferenceDb.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			// write the new OWL
			writeNewOwl(owlEngine);
		}
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws Exception 
	 */
	private void writeNewOwl(WriteOWLEngine owler) throws Exception {
		// ENGINE	
		for (Pair<String, List<Pair<String, String>>> columns : allSchemas) {
			String tableName = columns.getValue0();
			owler.addConcept(tableName, null, null);
			for (Pair<String, String> x : columns.getValue1()) {	
				owler.addProp(tableName, x.getValue0(), x.getValue1());
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
