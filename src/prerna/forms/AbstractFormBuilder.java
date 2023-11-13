package prerna.forms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractFormBuilder {

	protected static final Logger classLogger = LogManager.getLogger(AbstractFormBuilder.class);

	public static final String FORM_BUILDER_ENGINE_NAME = "form_builder_engine";
	protected static final String AUDIT_FORM_SUFFIX = "_FORM_LOG";
	protected static final String OVERRIDE = "override";
	protected static final String ADD = "Added";
	protected static final String REMOVE = "Removed";
	protected static final String ADMIN_SIGN_OFF = "Certified";

	protected IDatabaseEngine formEng;
	protected String auditLogTableName;
	protected String user;
	protected List<String> tagCols;
	protected List<String> tagValues;
	
	protected IDatabaseEngine engine;
	
	protected final DateFormat DATE_DF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS");
	
	protected AbstractFormBuilder(IDatabaseEngine engine) {
		this.formEng = Utility.getDatabase(FORM_BUILDER_ENGINE_NAME);
		this.engine = engine;
		this.auditLogTableName = AbstractSqlQueryUtil.escapeForSQLStatement(RDBMSEngineCreationHelper.cleanTableName(this.engine.getEngineId())).toUpperCase() + FormBuilder.AUDIT_FORM_SUFFIX;
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
	
	/**
	 * Utility method to generate a form user access table if it doesn't currently exist
	 * @param formEng
	 */
	public static void generateFormPermissionTable(IDatabaseEngine formEng) {
		// create audit table if doesn't exist
		boolean permissionTableExists = false;
		String checkTableQuery = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='FORMS_USER_ACCESS'";
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(formEng, checkTableQuery);
			if(wrapper.hasNext()) {
				permissionTableExists = true;
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
		
		if(!permissionTableExists) {
			try(WriteOWLEngine owlEngine = formEng.getOWLEngineFactory().getWriteOWL()) {
				owlEngine.addConcept("FORMS_USER_ACCESS", null, null);
				owlEngine.addProp("FORMS_USER_ACCESS", "USER_ID", "VARCHAR(100)");
				owlEngine.addProp("FORMS_USER_ACCESS", "INSTANCE_NAME", "VARCHAR(255)");
				owlEngine.addProp("FORMS_USER_ACCESS", "IS_SYS_ADMIN", "BOOLEAN");
	
				classLogger.info("CREATING PERMISSION TABLE!!!");
				String query = "CREATE TABLE FORM_USER_ACCESS (USER_ID VARCHAR(100), INSTANCE_NAME VARCHAR(255), IS_SYS_ADMIN BOOLEAN)";
				IRDBMSEngine rdbmsEng = (IRDBMSEngine) formEng;
				classLogger.info("SQL SCRIPT >>> " + query);
				Connection conn = null;
				Statement stmt = null;
				try {
					conn = rdbmsEng.getConnection();
					stmt = conn.createStatement();
					stmt.execute(query);
					if(!stmt.getConnection().getAutoCommit()) {
						stmt.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(rdbmsEng, conn, stmt, null);
				}
				try {
					owlEngine.commit();
					owlEngine.export();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			} catch (IOException | InterruptedException e1) {
				classLogger.error(Constants.STACKTRACE, e1);
			}
		}
	}
	
	protected void generateEngineAuditLog(String auditLogTableName) {
		// create audit table if doesn't exist
		boolean auditTableExists = false;
		String checkTableQuery = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='" + auditLogTableName + "'";
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(this.formEng, checkTableQuery);
			if(wrapper.hasNext()) {
				auditTableExists = true;
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
		
		if(!auditTableExists) {
			try(WriteOWLEngine owlEngine = formEng.getOWLEngineFactory().getWriteOWL()) {
				owlEngine.addConcept(auditLogTableName, null, null);
				owlEngine.addProp(auditLogTableName, "ID", "INT");
				owlEngine.addProp(auditLogTableName, "USER", "VARCHAR(255)");
				owlEngine.addProp(auditLogTableName, "ACTION", "VARCHAR(100)");
				owlEngine.addProp(auditLogTableName, "START_NODE", "VARCHAR(255)");
				owlEngine.addProp(auditLogTableName, "REL_NAME", "VARCHAR(255)");
				owlEngine.addProp(auditLogTableName, "END_NODE", "VARCHAR(255)");
				owlEngine.addProp(auditLogTableName, "PROP_NAME", "VARCHAR(255)");
				owlEngine.addProp(auditLogTableName, "PROP_VALUE", "CLOB");
				owlEngine.addProp(auditLogTableName, "TIME", "TIMESTAMP");
	
				classLogger.info("CREATING NEW AUDIT LOG!!!");
				StringBuilder createAuditTable = new StringBuilder("CREATE TABLE ");
				createAuditTable.append(auditLogTableName).append("(ID IDENTITY, USER VARCHAR(255), ACTION VARCHAR(100), START_NODE VARCHAR(255), "
						+ "REL_NAME VARCHAR(255), END_NODE VARCHAR(255), PROP_NAME VARCHAR(255), PROP_VALUE CLOB, TIME TIMESTAMP");
				if(this.tagCols != null) {
					for(String tag : tagCols) {
						createAuditTable.append(tag).append(" VARCHAR(100), ");
						owlEngine.addProp(auditLogTableName, "ID", tag.toUpperCase(), "VARCHAR(100)");
					}
				}
				createAuditTable.append(")");
				String query = createAuditTable.toString();
				classLogger.info("SQL SCRIPT >>> " + Utility.cleanLogString(query));
				try {
					this.formEng.insertData(query);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
				try {
					owlEngine.commit();
					owlEngine.export();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			} catch (IOException | InterruptedException e1) {
				classLogger.error(Constants.STACKTRACE, e1);
			}
			
		} else if(this.tagCols != null && this.tagCols.size() > 0){
			// need to execute and get the columns for the table
			// need to make sure it has the tag cols
			// since there can be multiple forms with different tags for searching on the same engine
			
			try(WriteOWLEngine owlEngine = formEng.getOWLEngineFactory().getWriteOWL()) {

				List<String> cols = new Vector<String>();
				// 1) query to get the current cols
				String allColsPresent = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + auditLogTableName.toUpperCase() + "'";
				try {
					wrapper = WrapperManager.getInstance().getRawWrapper(this.formEng, allColsPresent);
					while(wrapper.hasNext()) {
						cols.add(wrapper.next().getValues()[0] + "");
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
				// 2) find the cols that we need to add
				List<String> colsToAdd = new Vector<String>();
				List<String> colsToAddTypes = new Vector<String>();
				for(String tag : tagCols) {
					if(!cols.contains(tag)) {
						colsToAdd.add(tag);
						colsToAddTypes.add("VARCHAR(100)");
						owlEngine.addProp(auditLogTableName, tag.toUpperCase(), "VARCHAR(100)");
					}
				}
				
				// 3) perform an update
				if(colsToAdd.size() > 0) {
					IRDBMSEngine rdbmsEng = (IRDBMSEngine) this.formEng;
					Connection conn = null;
					Statement stmt = null;
					String alterQuery = rdbmsEng.getQueryUtil().alterTableAddColumns(auditLogTableName, colsToAdd.toArray(new String[] {}), colsToAddTypes.toArray(new String[] {}));
					classLogger.info("ALTERING TABLE: " + Utility.cleanLogString(alterQuery));
					try {
						conn = rdbmsEng.getConnection();
						stmt = conn.createStatement();
						stmt.execute(alterQuery);
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						ConnectionUtils.closeAllConnectionsIfPooling(rdbmsEng, conn, stmt, null);
					}
					classLogger.info("DONE ALTER TABLE");
					
					try {
						owlEngine.commit();
						owlEngine.export();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			} catch (IOException | InterruptedException e1) {
				classLogger.error(Constants.STACKTRACE, e1);
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
			cleanUser = this.user;
		} else {
			cleanUser = "User Information Not Submitted";
		}
		IRDBMSEngine rdbmsEng = (IRDBMSEngine) formEng;
		
		PreparedStatement ps = null;

		try {
			ps = rdbmsEng.getPreparedStatement("INSERT INTO "+this.auditLogTableName+" (USER, ACTION, START_NODE, REL_NAME, END_NODE, PROP_NAME, PROP_VALUE, TIME) VALUES(?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, cleanUser);
			ps.setString(parameterIndex++, action);
			ps.setString(parameterIndex++, startNode);
			ps.setString(parameterIndex++, relName);
			ps.setString(parameterIndex++, endNode);
			ps.setString(parameterIndex++, propName);
			ps.setString(parameterIndex++, propValue);
			ps.setString(parameterIndex++, timeStamp);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(rdbmsEng, ps);
		}
	}

	public void setUser(String user) {
		this.user = user;
	}
}
