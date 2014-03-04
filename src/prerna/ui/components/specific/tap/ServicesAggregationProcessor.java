package prerna.ui.components.specific.tap;

import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.UpdateProcessor;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

public class ServicesAggregationProcessor {

	Logger logger = Logger.getLogger(getClass());
	IEngine servicesDB;
	IEngine coreDB;
	String baseURI;
	
	public ServicesAggregationProcessor(IEngine servicesDB, IEngine coreDB, String baseURI){
		this.servicesDB = servicesDB;
		this.coreDB = coreDB;
		this.baseURI = baseURI;
	}
	
	public void runFullAggregation(){
		runSystemServicePropertyAggregation();
		runHardwareAggregation();
		runSoftwareAggregation();
		runPersonnelAggregation();
		runBusinessProcessAggregation();
		runActivityAggregation();
		runBLUAggregation();
		runDataObjectAggregation();
		runTErrorAggregation();
		runICDAggregation();
	}
	
	/*
	 * -Full System Name – Concatenate service entries if different
		-Description – Concatenate service entries if different
		-Number of Users - Add
		-User Consoles – Add
		-Availability-Required – Use highest
		-Availability-Actual – Use lowest
		-Transaction Count - Add
		-ATO Date – Use earliest
		-GarrisonTheater – If both Garrison and Theater, label, “Both”
		-End of Support Date – Take latest
		-POC - Concatenate
		-Transactional – Flag and check with POCs
	 */
	private void runSystemServicePropertyAggregation(){
//		String propQuery = DIHelper.getInstance().getLocalProp(ConstantsTAP.queryname);
//		SesameJenaSelectWrapper sjsw = processQuery(propQuery, servicesDB);
//		Hashtable<String, Hashtable<String, String>> sysPropHash = new Hashtable<String, Hashtable<String, String>>() ;
//		while(sjsw.hasNext()){
//			SesameJenaSelectStatement sjss = sjsw.next();
//			// get the next row and see how it must be added to the insert query
//			String system = sjss.getRawVar("System") + "";
//			String prop = sjss.getRawVar("prop") + "";
//			String val = sjss.getRawVar("val") + "";
//			sysPropHash = addToHashtable(system, prop, val, sysPropHash);
//			
//		}
//		
//		String insertQuery = prepareInsertQuery(sysPropHash);
//		runInsert(insertQuery, coreDB);
	}
	
	/*
	 * -HardwareModule, HardwareVersion, Hardware – Roll up from service to enterprise level
		-Add quantities
	 */
	private void runHardwareAggregation(){
		
	}
	
	/*
	 * -SoftwareModule, SoftwareVersion, Software – Roll up from service to enterprise level
		-Add quantities
	 */
	private void runSoftwareAggregation(){
		
	}

	/*
	 * Accumulate
	 */
	private void runPersonnelAggregation(){
		
	}

	/*
	 * Accumulate
	 */
	private void runBusinessProcessAggregation(){
		
	}

	/*
	 * Accumulate
	 */
	private void runActivityAggregation(){
		
	}

	/*
	 * Accumulate
	 */
	private void runBLUAggregation(){
		
	}
	/*
	 * -DataObject – Aggregate, and “C” takes precedence over “M” or "R"
	 */
	private void runDataObjectAggregation(){
		
	}
	
	/*
	 * -TError – Aggregate, and divide %’s by the number of services reporting
	 */
	private void runTErrorAggregation(){
		
	}
	
	/*
	 * -InterfaceControlDocument – Copy over to TAP Core
		-DFreq – Keep the highest frequency (e.g. daily takes precedence over weekly, if we receive both)
		-DProt, DForm – Concatenate
		-Concatenate all properties
	 */
	private void runICDAggregation(){
		
	}
	
	//process the query
	private SesameJenaSelectWrapper processQuery(String query, IEngine engine){
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		
		
		return sjsw;
	}
	
	//this function will add a value to a hashtable in format {subject : {predicate : object}} which is what will be needed for the prepareInsertQuery function
	//need to think through how the aggregation will work if the s p o is already present in the hash... separate functions?
	private Hashtable<String, Hashtable<String, String>> addToHashtable(String key1, String key2, String val, Hashtable<String, Hashtable<String, String>> table){
		if (table.containsKey(key1)){
			Hashtable innerHash = table.get(key1);
			if(innerHash.containsKey(key2)) {
				
			}
			else {
				
			}
		}
		else { 
			
		}
		
		return table;
	}
	
	//this function will take a hashtable in the format {subject : {predicate : object}} to create an insert query
	private String prepareInsertQuery(Hashtable<String, Hashtable<String, String>> table){
		String insertQuery = "INSERT DATA { " ;
		
		return insertQuery;
	}
	
	// simply run the insert query
	private void runInsert(String query, IEngine engine){
		logger.info("Running update query into " + engine.getEngineName() + "::: " + query);
		UpdateProcessor upProc = new UpdateProcessor();
		upProc.setEngine(engine);
		upProc.setQuery(query);
		upProc.processQuery();
	}
}
