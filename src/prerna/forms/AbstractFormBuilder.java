package prerna.forms;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.OWLER;
import prerna.util.Utility;

public abstract class AbstractFormBuilder {

	protected static final Logger LOGGER = LogManager.getLogger(AbstractFormBuilder.class.getName());

	public static final String FORM_BUILDER_ENGINE_NAME = "form_builder_engine";
	protected static final String AUDIT_FORM_SUFFIX = "_FORM_LOG";
	protected static final DateFormat DATE_DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS");
	protected static final String OVERRIDE = "override";
	protected static final String ADD = "Added";
	protected static final String REMOVE = "Removed";
	protected static final String ADMIN_SIGN_OFF = "Certified";

	protected IEngine formEng;
	protected String auditLogTableName;
	protected String user;
	protected List<String> tagCols;
	protected List<String> tagValues;
	
	protected IEngine engine;
	
	protected AbstractFormBuilder(IEngine engine) {
		this.formEng = Utility.getEngine(FORM_BUILDER_ENGINE_NAME);
		this.engine = engine;
		this.auditLogTableName = RdbmsQueryBuilder.escapeForSQLStatement(RDBMSEngineCreationHelper.cleanTableName(this.engine.getEngineId())).toUpperCase() + FormBuilder.AUDIT_FORM_SUFFIX;
		generateEngineAuditLog(this.auditLogTableName);
	}
	
	public void commitFormData(Map<String, Object> engineHash, String user) throws IOException {
		this.user = user;
		
		if(engineHash.containsKey("tagCols") && engineHash.containsKey("tagValues")) {
			this.tagCols = (List<String>) engineHash.get("tagCols");
			this.tagValues = (List<String>) engineHash.get("tagValues");
		}
		
		String semossBaseURI = "http://semoss.org/ontologies";
		String baseURI = engine.getNodeBaseUri();
		if(baseURI != null && !baseURI.isEmpty()) {
			baseURI = baseURI.replace("/Concept/", "");
		} else {
			baseURI = semossBaseURI;
		}

		String relationBaseURI = semossBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		String conceptBaseURI = semossBaseURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String propertyBaseURI = semossBaseURI + "/" + Constants.DEFAULT_PROPERTY_CLASS;

		List<HashMap<String, Object>> nodes = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("nodes")) {
			nodes = (List<HashMap<String, Object>>) engineHash.get("nodes"); 
		}
		List<HashMap<String, Object>> relationships = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("relationships")) {
			relationships = (List<HashMap<String, Object>>)engineHash.get("relationships");
		}
		List<HashMap<String, Object>> removeNodes = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("removeNodes")) {
			removeNodes = (List<HashMap<String, Object>>) engineHash.get("removeNodes"); 
		}
		List<HashMap<String, Object>> removeRelationships = new ArrayList<HashMap<String, Object>>();
		if(engineHash.containsKey("removeRelationships")) {
			removeRelationships = (List<HashMap<String, Object>>)engineHash.get("removeRelationships");
		}

		saveFormData(baseURI, conceptBaseURI, relationBaseURI, propertyBaseURI, nodes, relationships, removeNodes, removeRelationships);

		//commit information to db
		this.formEng.commit();
		this.engine.commit();
	}
	
	protected void generateEngineAuditLog(String auditLogTableName) {
		// create audit table if doesn't exist
		String checkTableQuery = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='" + auditLogTableName + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(this.formEng, checkTableQuery);
		boolean auditTableExists = false;
		if(wrapper.hasNext()) {
			auditTableExists = true;
			// call the next so we close the rs
			wrapper.next();
		}
		if(!auditTableExists) {
			OWLER owler = new OWLER(this.formEng, this.formEng.getOWL());
			owler.addConcept(auditLogTableName, "ID", "INT");
			owler.addProp(auditLogTableName, "ID", "USER", "VARCHAR(255)");
			owler.addProp(auditLogTableName, "ID", "ACTION", "VARCHAR(100)");
			owler.addProp(auditLogTableName, "ID", "START_NODE", "VARCHAR(255)");
			owler.addProp(auditLogTableName, "ID", "REL_NAME", "VARCHAR(255)");
			owler.addProp(auditLogTableName, "ID", "END_NODE", "VARCHAR(255)");
			owler.addProp(auditLogTableName, "ID", "PROP_NAME", "VARCHAR(255)");
			owler.addProp(auditLogTableName, "ID", "PROP_VALUE", "CLOB");
			owler.addProp(auditLogTableName, "ID", "TIME", "TIMESTAMP");

			LOGGER.info("CREATING NEW AUDIT LOG!!!");
			StringBuilder createAuditTable = new StringBuilder("CREATE TABLE ");
			createAuditTable.append(auditLogTableName).append("(ID IDENTITY, USER VARCHAR(255), ACTION VARCHAR(100), START_NODE VARCHAR(255), "
					+ "REL_NAME VARCHAR(255), END_NODE VARCHAR(255), PROP_NAME VARCHAR(255), PROP_VALUE CLOB, TIME TIMESTAMP");
			if(this.tagCols != null) {
				for(String tag : tagCols) {
					createAuditTable.append(tag).append(" VARCHAR(100), ");
					owler.addProp(auditLogTableName, "ID", tag.toUpperCase(), "VARCHAR(100)");
				}
			}
			createAuditTable.append(")");
			String query = createAuditTable.toString();
			LOGGER.info("SQL SCRIPT >>> " + query);
			try {
				this.formEng.insertData(query);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			owler.commit();
			try {
				owler.export();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else if(this.tagCols != null && this.tagCols.size() > 0){
			// need to execute and get the columns for the table
			// need to make sure it has the tag cols
			// since there can be multiple forms with different tags for searching on the same engine
			
			OWLER owler = new OWLER(this.formEng, this.formEng.getOWL());

			// 1) query to get the current cols
			String allColsPresent = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + auditLogTableName.toUpperCase() + "'";
			wrapper = WrapperManager.getInstance().getRawWrapper(this.formEng, allColsPresent);
			List<String> cols = new Vector<String>();
			while(wrapper.hasNext()) {
				cols.add(wrapper.next().getValues()[0] + "");
			}
			// 2) find the cols that we need to add
			List<String> colsToAdd = new Vector<String>();
			List<String> colsToAddTypes = new Vector<String>();
			for(String tag : tagCols) {
				if(!cols.contains(tag)) {
					colsToAdd.add(tag);
					colsToAddTypes.add("VARCHAR(100)");
					owler.addProp(auditLogTableName, "ID", tag.toUpperCase(), "VARCHAR(100)");
				}
			}
			
			// 3) perform an update
			if(colsToAdd.size() > 0) {
				String alterQuery = RdbmsQueryBuilder.makeAlter(auditLogTableName, colsToAdd.toArray(new String[] {}), colsToAddTypes.toArray(new String[] {}));
				LOGGER.info("ALTERING TABLE: " + alterQuery);
				try {
					this.formEng.insertData(alterQuery);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				LOGGER.info("DONE ALTER TABLE");
				
				owler.commit();
				try {
					owler.export();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Method to actually save form data
	 * @param baseURI
	 * @param conceptBaseURI
	 * @param relationBaseURI
	 * @param propertyBaseURI
	 * @param nodes
	 * @param relationships
	 * @param removeNodes
	 * @param removeRelationships
	 */
	protected abstract void saveFormData(
			String baseURI, 
			String conceptBaseURI,
			String relationBaseURI, 
			String propertyBaseURI,
			List<HashMap<String, Object>> nodes, 
			List<HashMap<String, Object>> relationships,
			List<HashMap<String, Object>> removeNodes, 
			List<HashMap<String, Object>> removeRelationships);


	/**
	 * Method to replace an instance across the entire db
	 * @param origName
	 * @param newName
	 * @param deleteInstanceBoolean
	 */
	public abstract void modifyInstanceValue(String origName, String newName, boolean deleteInstanceBoolean);
	
	public abstract void certifyInstance(String conceptType, String instanceName);
	
	/**
	 * Store the action that was performed in the audit log
	 * @param action
	 * @param startNode
	 * @param relName
	 * @param endNode
	 * @param propName
	 * @param propValue
	 * @param timeStamp
	 */
	protected void addAuditLog(String action, String startNode, String relName, String endNode, String propName, String propValue, String timeStamp) {
		String cleanUser = null;
		// TODO: FE NEEDS TO PASS IN USER!
		if(this.user != null) {
			cleanUser = RdbmsQueryBuilder.escapeForSQLStatement(this.user);
		} else {
			cleanUser = "User Information Not Submitted";
		}
		
		startNode = RdbmsQueryBuilder.escapeForSQLStatement(startNode);
		relName = RdbmsQueryBuilder.escapeForSQLStatement(relName);
		endNode = RdbmsQueryBuilder.escapeForSQLStatement(endNode);
		propName = RdbmsQueryBuilder.escapeForSQLStatement(propName);
		propValue = RdbmsQueryBuilder.escapeForSQLStatement(propValue);

		String valuesBreak = "', '";
		StringBuilder insertLogStatement = new StringBuilder("INSERT INTO ");
		insertLogStatement.append(this.auditLogTableName).append("(USER, ACTION, START_NODE, REL_NAME, END_NODE, PROP_NAME, PROP_VALUE, TIME) VALUES('")
						.append(cleanUser).append(valuesBreak).append(action).append(valuesBreak).append(startNode).append(valuesBreak)
						.append(relName).append(valuesBreak).append(endNode).append(valuesBreak).append(propName).append(valuesBreak)
						.append(propValue).append(valuesBreak).append(timeStamp).append("')");
		try {
			this.formEng.insertData(insertLogStatement.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setUser(String user) {
		this.user = user;
	}
}
