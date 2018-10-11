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
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public abstract class AbstractMetaEditorReactor extends AbstractReactor {

	protected String getAppId(String appId) {
		String testId = appId;
		if(AbstractSecurityUtils.securityEnabled()) {
			testId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), testId);
			if(!SecurityQueryUtils.userCanEditEngine(this.insight.getUser(), testId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist or user does not have access to app");
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
}
