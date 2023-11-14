package prerna.usertracking;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class UserTrackingOwlCreator {
	
	// each column name paired to its type in a var
	private List<Pair<String, String>> userTrackingColumns = null;
	private List<Pair<String, String>> engineViewsColumns = null;
	private List<Pair<String, String>> engineUsesColumns = null;
	private List<Pair<String, String>> userCatalogVotes = null;
	private List<Pair<String, String>> emailTracking = null;
	private List<Pair<String, String>> insightOpens = null;
	private List<Pair<String, String>> queryTrackingColumns = null;
	// Pairs table name with its respective columns
	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;
	
	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new Vector<String>();
	static {
		conceptsRequired.add("USER_TRACKING");
		conceptsRequired.add("ENGINE_VIEWS");
		conceptsRequired.add("ENGINE_USES");
		conceptsRequired.add("USER_CATALOG_VOTES");
		conceptsRequired.add("EMAIL_TRACKING");
		conceptsRequired.add("INSIGHT_OPENS");
		conceptsRequired.add("QUERY_TRACKING");
	}
	
	private IRDBMSEngine userTrackingDb;
	
	public UserTrackingOwlCreator(IRDBMSEngine userTrackingDb) {
		this.userTrackingDb = userTrackingDb;
		createColumnsAndTypes(this.userTrackingDb.getQueryUtil());
	}
	
	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		this.userTrackingColumns = Arrays.asList(
				Pair.with("SESSIONID", "VARCHAR(255)"),
				Pair.with("USERID", "VARCHAR(255)"),
				Pair.with("TYPE", "VARCHAR(255)"),
				Pair.with("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("IP_ADDR", "VARCHAR(255)"),
				Pair.with("IP_LAT", "VARCHAR(255)"),
				Pair.with("IP_LONG", "VARCHAR(255)"),
				Pair.with("IP_COUNTRY", "VARCHAR(255)"),
				Pair.with("IP_STATE", "VARCHAR(255)"),
				Pair.with("IP_CITY", "VARCHAR(255)")            
			);
		
		this.engineViewsColumns = Arrays.asList(
				Pair.with("ENGINEID", "VARCHAR(255)"),
				Pair.with("DATE", "DATE"),
				Pair.with("VIEWS", "INT")
			);
		
		this.engineUsesColumns = Arrays.asList(
				Pair.with("ENGINEID", "VARCHAR(255)"),
				Pair.with("INSIGHTID", "VARCHAR(255)"),
				Pair.with("PROJECTID", "VARCHAR(255)"),
				Pair.with("DATE", "DATE")
			);
		
		this.userCatalogVotes = Arrays.asList(
				Pair.with("USERID", "VARCHAR(255)"),
				Pair.with("TYPE", "VARCHAR(255)"),
				Pair.with("ENGINEID", "VARCHAR(255)"),
				Pair.with("VOTE", "INT"),
				Pair.with("LAST_MODIFIED", TIMESTAMP_DATATYPE_NAME)
			);
		
		this.emailTracking = Arrays.asList(
				Pair.with("ID", "VARCHAR(255)"),
				Pair.with("SENT_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("SUCCESSFUL", BOOLEAN_DATATYPE_NAME),
				Pair.with("E_FROM", "VARCHAR(255)"),
				Pair.with("E_TO", CLOB_DATATYPE_NAME),
				Pair.with("E_CC", CLOB_DATATYPE_NAME),
				Pair.with("E_BCC", CLOB_DATATYPE_NAME),
				Pair.with("E_SUBJECT", "VARCHAR(1000)"),
				Pair.with("BODY", CLOB_DATATYPE_NAME),
				Pair.with("ATTACHMENTS", CLOB_DATATYPE_NAME),
				Pair.with("IS_HTML", BOOLEAN_DATATYPE_NAME)
			);
		
		this.insightOpens = Arrays.asList(
				Pair.with("INSIGHTID", "VARCHAR(255)"),
				Pair.with("USERID", "VARCHAR(255)"),
				Pair.with("OPENED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ORIGIN", "VARCHAR(2000)")
				);
		
		this.queryTrackingColumns = Arrays.asList(
				Pair.with("ID", "VARCHAR(255)"),
				Pair.with("USERID", "VARCHAR(255)"),
				Pair.with("USERTYPE", "VARCHAR(255)"),
				Pair.with("DATABASEID", "VARCHAR(255)"),
				Pair.with("QUERY_EXECUTED", CLOB_DATATYPE_NAME),
				Pair.with("START_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("END_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.with("TOTAL_EXECUTION_TIME", "BIGINT"),
				Pair.with("FAILED_EXECUTION", BOOLEAN_DATATYPE_NAME)
				);
		
		this.allSchemas = Arrays.asList(
				Pair.with("USER_TRACKING", userTrackingColumns),
				Pair.with("ENGINE_VIEWS", engineViewsColumns),
				Pair.with("ENGINE_USES", engineUsesColumns),
				Pair.with("USER_CATALOG_VOTES", userCatalogVotes),
				Pair.with("EMAIL_TRACKING", emailTracking),
				Pair.with("INSIGHT_OPENS", insightOpens),
				Pair.with("QUERY_TRACKING", queryTrackingColumns)
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
		List<String> concepts = userTrackingDb.getPhysicalConcepts();
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

		List<String> props = userTrackingDb.getPropertyUris4PhysicalUri(propsURI);	
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
		try(WriteOWLEngine owlEngine = userTrackingDb.getOWLEngineFactory().getWriteOWL()) {
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
}
