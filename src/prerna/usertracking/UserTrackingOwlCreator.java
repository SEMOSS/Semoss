package prerna.usertracking;

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
	
	private IRDBMSEngine sessionDb;
	
	public UserTrackingOwlCreator(IRDBMSEngine sessionDb) {
		this.sessionDb = sessionDb;
		createColumnsAndTypes(this.sessionDb.getQueryUtil());
	}
	
	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		this.userTrackingColumns = Arrays.asList(
				Pair.of("SESSIONID", "VARCHAR(255)"),
				Pair.of("USERID", "VARCHAR(255)"),
				Pair.of("TYPE", "VARCHAR(255)"),
				Pair.of("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.of("ENDED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.of("IP_ADDR", "VARCHAR(255)"),
				Pair.of("IP_LAT", "VARCHAR(255)"),
				Pair.of("IP_LONG", "VARCHAR(255)"),
				Pair.of("IP_COUNTRY", "VARCHAR(255)"),
				Pair.of("IP_STATE", "VARCHAR(255)"),
				Pair.of("IP_CITY", "VARCHAR(255)")            
			);
		
		this.engineViewsColumns = Arrays.asList(
				Pair.of("ENGINEID", "VARCHAR(255)"),
				Pair.of("DATE", "DATE"),
				Pair.of("VIEWS", "INT")
			);
		
		this.engineUsesColumns = Arrays.asList(
				Pair.of("ENGINEID", "VARCHAR(255)"),
				Pair.of("INSIGHTID", "VARCHAR(255)"),
				Pair.of("PROJECTID", "VARCHAR(255)"),
				Pair.of("DATE", "DATE")
			);
		
		this.userCatalogVotes = Arrays.asList(
				Pair.of("USERID", "VARCHAR(255)"),
				Pair.of("TYPE", "VARCHAR(255)"),
				Pair.of("ENGINEID", "VARCHAR(255)"),
				Pair.of("VOTE", "INT"),
				Pair.of("LAST_MODIFIED", TIMESTAMP_DATATYPE_NAME)
			);
		
		this.emailTracking = Arrays.asList(
				Pair.of("ID", "VARCHAR(255)"),
				Pair.of("SENT_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.of("SUCCESSFUL", BOOLEAN_DATATYPE_NAME),
				Pair.of("E_FROM", "VARCHAR(255)"),
				Pair.of("E_TO", CLOB_DATATYPE_NAME),
				Pair.of("E_CC", CLOB_DATATYPE_NAME),
				Pair.of("E_BCC", CLOB_DATATYPE_NAME),
				Pair.of("E_SUBJECT", "VARCHAR(1000)"),
				Pair.of("BODY", CLOB_DATATYPE_NAME),
				Pair.of("ATTACHMENTS", CLOB_DATATYPE_NAME),
				Pair.of("IS_HTML", BOOLEAN_DATATYPE_NAME)
			);
		
		this.insightOpens = Arrays.asList(
				Pair.of("INSIGHTID", "VARCHAR(255)"),
				Pair.of("USERID", "VARCHAR(255)"),
				Pair.of("OPENED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.of("ORIGIN", "VARCHAR(2000)")
				);
		
		this.queryTrackingColumns = Arrays.asList(
				Pair.of("ID", "VARCHAR(255)"),
				Pair.of("USERID", "VARCHAR(255)"),
				Pair.of("USERTYPE", "VARCHAR(255)"),
				Pair.of("DATABASEID", "VARCHAR(255)"),
				Pair.of("QUERY_EXECUTED", CLOB_DATATYPE_NAME),
				Pair.of("START_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.of("END_TIME", TIMESTAMP_DATATYPE_NAME),
				Pair.of("TOTAL_EXECUTION_TIME", "BIGINT"),
				Pair.of("FAILED_EXECUTION", BOOLEAN_DATATYPE_NAME)
				);
		
		this.allSchemas = Arrays.asList(
				Pair.of("USER_TRACKING", userTrackingColumns),
				Pair.of("ENGINE_VIEWS", engineViewsColumns),
				Pair.of("ENGINE_USES", engineUsesColumns),
				Pair.of("USER_CATALOG_VOTES", userCatalogVotes),
				Pair.of("EMAIL_TRACKING", emailTracking),
				Pair.of("INSIGHT_OPENS", insightOpens),
				Pair.of("QUERY_TRACKING", queryTrackingColumns)
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
		List<String> concepts = sessionDb.getPhysicalConcepts();
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

		List<String> props = sessionDb.getPropertyUris4PhysicalUri(propsURI);	
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
		RDFFileSesameEngine baseEngine = sessionDb.getBaseDataEngine();
		if(baseEngine != null) {
			baseEngine.close();
		}
		
		// now delete the file if exists
		// and we will make a new
		String owlLocation = sessionDb.getOWL();
		
		File f = new File(owlLocation);
		if(f.exists()) {
			f.delete();
		}
		
		// write the new OWL
		writeNewOwl(owlLocation);
		
		// now reload into security db
		sessionDb.setOWL(owlLocation);
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
}
