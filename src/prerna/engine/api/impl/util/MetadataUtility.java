package prerna.engine.api.impl.util;

import java.util.List;

import prerna.engine.api.IEngine;
import prerna.util.Utility;

public class MetadataUtility {

	private MetadataUtility() {
		
	}
	
	public static boolean ignoreConceptData(IEngine.ENGINE_TYPE type) {
		return type == IEngine.ENGINE_TYPE.RDBMS
				|| type == IEngine.ENGINE_TYPE.IMPALA
				|| type == IEngine.ENGINE_TYPE.R
//				|| type == IEngine.ENGINE_TYPE.JMES_API
				;
	}
	
	/**
	 * Check if a given property name is already present in an existing concept
	 * @param engine
	 * @param conceptPhysicalUri
	 * @param propertyName
	 * @return
	 */
	public static boolean propertyExistsForConcept(IEngine engine, String conceptPhysicalUri, String propertyName) {
		List<String> properties = engine.getPropertyUris4PhysicalUri(conceptPhysicalUri);
		for(String prop : properties) {
			if(propertyName.equalsIgnoreCase(Utility.getInstanceName(prop))) {
				return true;
			}
		}
		
		return false;
	}
	
//	/**
//	 * Get an owler with the filled stores
//	 * @param engine
//	 * @return
//	 */
//	public static Owler getFilledOwler(IEngine engine) {
//		Owler owler = new Owler(engine);
//		Hashtable<String, String> conceptHash = new Hashtable<String, String>();
//		Hashtable<String, String> propHash = new Hashtable<String, String>();
//		Hashtable<String, String> relationHash = new Hashtable<String, String>();
//
//		boolean isRdbms = (engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS || 
//				engine.getEngineType() == IEngine.ENGINE_TYPE.IMPALA);
//
//		List<String> concepts = engine.getPhysicalConcepts();
//		for(String c : concepts) {
//			String tableName = Utility.getInstanceName(c);
//			String cKey = tableName;
//			if(isRdbms) {
//				cKey = Utility.getClassName(c) + cKey;
//			}
//			// add to concept hash
//			conceptHash.put(cKey, c);
//
//			// add all the props as well
//			List<String> props = engine.getPropertyUris4PhysicalUri(c);
//			for(String p : props) {
//				String propName = null;
//				if(isRdbms) {
//					propName = Utility.getClassName(p);
//				} else {
//					propName = Utility.getInstanceName(p);
//				}
//
//				propHash.put(tableName + "%" + propName, p);
//			}
//		}
//
//		List<String[]> rels = engine.getPhysicalRelationships();
//		for(String[] r : rels) {
//			String startT = null;
//			String startC = null;
//			String endT = null;
//			String endC = null;
//			String pred = null;
//
//			startT = Utility.getInstanceName(r[0]);
//			endT = Utility.getInstanceName(r[1]);
//			pred = Utility.getInstanceName(r[2]);
//
//			if(isRdbms) {
//				startC = Utility.getClassName(r[0]);
//				endC = Utility.getClassName(r[1]);
//			}
//
//			relationHash.put(startT + startC + endT + endC + pred, r[2]);
//		}
//
//		owler.setConceptHash(conceptHash);
//		owler.setPropHash(propHash);
//		owler.setRelationHash(relationHash);
//		
//		return owler;
//	}
	
}
