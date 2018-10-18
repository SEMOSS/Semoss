package prerna.sablecc2.reactor.app.metaeditor;

import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public abstract class AbstractMetaEditorReactor extends AbstractReactor {

	protected static final String TABLES_FILTER = ReactorKeysEnum.TABLES.getKey();

	protected String getAppId(String appId, boolean edit) {
		String testId = appId;
		if(AbstractSecurityUtils.securityEnabled()) {
			testId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), testId);
			if(edit) {
				// need edit permission
				if(!SecurityQueryUtils.userCanEditEngine(this.insight.getUser(), testId)) {
					throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to app");
				}
			} else {
				// just need read access
				if(!SecurityQueryUtils.getUserEngineIds(this.insight.getUser()).contains(testId)) {
					throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to app");
				}
			}
		} else {
			testId = MasterDatabaseUtility.testEngineIdIfAlias(testId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(testId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}
		return testId;
	}

	protected RDFFileSesameEngine loadOwlEngineFile(String appId) {
		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(appId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);
		String owlFile = SmssUtilities.getOwlFile(prop).getAbsolutePath();

		// owl is stored as RDF/XML file
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owlFile, null, null);
		return rfse;
	}

	protected OWLER getOWLER(String appId) {
		IEngine app = Utility.getEngine(appId);
		OWLER owler = new OWLER(app, app.getOWL());
		return owler;
	}

	/**
	 * Get values to fill in the OWLER as we query for correct uris based
	 * on the type of operation we are performing
	 * @param engine
	 * @param owler
	 */
	protected void setOwlerValues(IEngine engine, OWLER owler) {
		Hashtable<String, String> conceptHash = new Hashtable<String, String>();
		Hashtable<String, String> propHash = new Hashtable<String, String>();
		Hashtable<String, String> relationHash = new Hashtable<String, String>();

		boolean isRdbms = (engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS || 
				engine.getEngineType() == IEngine.ENGINE_TYPE.IMPALA);

		Vector<String> concepts = engine.getConcepts(false);
		for(String c : concepts) {
			String tableName = Utility.getInstanceName(c);
			String cKey = tableName;
			if(isRdbms) {
				cKey = Utility.getClassName(c) + cKey;
			}
			// add to concept hash
			conceptHash.put(cKey, c);

			// add all the props as well
			List<String> props = engine.getProperties4Concept(c, false);
			for(String p : props) {
				String propName = null;
				if(isRdbms) {
					propName = Utility.getClassName(p);
				} else {
					propName = Utility.getInstanceName(p);
				}

				propHash.put(tableName + "%" + propName, p);
			}
		}

		Vector<String[]> rels = engine.getRelationships(false);
		for(String[] r : rels) {
			String startT = null;
			String startC = null;
			String endT = null;
			String endC = null;
			String pred = null;

			startT = Utility.getInstanceName(r[0]);
			endT = Utility.getInstanceName(r[1]);
			pred = Utility.getInstanceName(r[2]);

			if(isRdbms) {
				startC = Utility.getClassName(r[0]);
				endC = Utility.getClassName(r[1]);
			}

			relationHash.put(startT + startC + endT + endC + pred, r[2]);
		}

		owler.setConceptHash(conceptHash);
		owler.setPropHash(propHash);
		owler.setRelationHash(relationHash);
	}

	/**
	 * Get the base folder
	 * @return
	 */
	protected String getBaseFolder() {
		String baseFolder = null;
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			// logger.info("No BaseFolder detected... most likely running as test...");
		}
		return baseFolder;
	}

	/**
	 * Get a list of tables to run certain routines
	 * @return
	 */
	protected List<String> getTableFilters() {
		List<String> filters = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(TABLES_FILTER);
		if(grs !=  null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				filters.add(grs.get(i).toString());
			}
		}

		if(filters.size() == 1) {
			throw new IllegalArgumentException("Must define at least 2 tables");
		}

		return filters;
	}

	/**
	 * Get an array of lists
	 * The first list contains the tables
	 * The second list contains the column
	 * But the first list table will repeat for each column
	 * so that they match based on index
	 */
	protected List<String>[] getTablesAndColumnsList(IEngine app, List<String> tableFilters) {
		// store 2 lists
		// of all table names
		// and column names
		// matched by index
		List<String> tableNamesList = new Vector<String>();
		List<String> columnNamesList = new Vector<String>();

		Vector<String> concepts = app.getConcepts(false);
		for(String cUri : concepts) {
			String tableName = Utility.getInstanceName(cUri);
			String tablePrimCol = Utility.getClassName(cUri);

			// if this is empty
			// no filters have been defined
			if(!tableFilters.isEmpty()) {
				// now if the table isn't included
				// ignore it
				if(!tableFilters.contains(tableName)) {
					continue;
				}
			}

			tableNamesList.add(tableName);
			columnNamesList.add(tablePrimCol);

			// grab all the properties
			List<String> properties = app.getProperties4Concept(cUri, false);
			for(String pUri : properties) {
				tableNamesList.add(tableName);
				columnNamesList.add(Utility.getClassName(pUri));
			}
		}

		return new List[]{tableNamesList, columnNamesList};
	}
	
	protected SelectQueryStruct getSingleColumnNonEmptyQs(String qsName, int limit) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(qsName));
		qs.setLimit(limit);
		
		{
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(qsName), PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata(null, PixelDataType.NULL_VALUE);
			SimpleQueryFilter f = new SimpleQueryFilter(lComparison, "!=", rComparison );
			qs.addExplicitFilter(f);
		}
		{
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(qsName), PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata("", PixelDataType.CONST_STRING);
			SimpleQueryFilter f = new SimpleQueryFilter(lComparison, "!=", rComparison );
			qs.addExplicitFilter(f);
		}
		
		return qs;
	}

}
