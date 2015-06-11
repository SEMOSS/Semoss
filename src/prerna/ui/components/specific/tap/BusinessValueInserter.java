package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.ui.components.UpdateProcessor;
import prerna.util.DIHelper;

public class BusinessValueInserter {
	double soaAlphaValue;
	
	static final Logger logger = LogManager.getLogger(BusinessValueInserter.class.getName());

	/**
	 * Calculates and inserts the Business Value of all systems based on all capabilities.
	 *  The result is inserted BusinessValue property in the System Properties.
	 *  
	 */
	public void runCalculationsAndInsertions(){
		inserter(calculateBusinessFunction(), "BusinessValue");
	}
	
	/**
	 * Takes in System and property to be inserted as a HashMap and names the property according to propName
	 * 
	 * @param tmHash
	 *            HashMap with System name as key and Technical Maturity score as value
	 * @param propName
	 *            Name of property
	 */
	private void inserter(HashMap<String, Double> bvHash, String propName) {
		IEngine tapCoreData = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		
		// Loop through entire hashmap and create a giant insert query to be run by UpdateProcessor
		String insertQuery = prepareInsert(bvHash, propName);
		
		UpdateProcessor pro = new UpdateProcessor();
		pro.setQuery(insertQuery);
		pro.setEngine(tapCoreData);
		pro.processQuery();
	}
	
	/**
	 * Loops through HashMap and generates Insert query that will insert property value on each system node
	 * 
	 * @param systemTMHash
	 *            HashMap containing System name as key and Technical Maturity score as value
	 * @param propName
	 *            Name of property
	 * @return String of Insert query
	 */
	private String prepareInsert(HashMap<String, Double> systemTMHash, String propName) {
		String predUri = "<http://semoss.org/ontologies/Relation/Contains/" + propName + ">";
		
		// add start with type triple
		String insertQuery = "INSERT DATA { " + predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
				+ "<http://semoss.org/ontologies/Relation/Contains>. ";
		
		// add other type triple
		insertQuery = insertQuery + predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
				+ "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
		
		// add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
		insertQuery = insertQuery + predUri + " <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " + predUri + ". ";
		
		for (String system : systemTMHash.keySet()) {
			String subjectUri = "<http://health.mil/ontologies/Concept/System/" + system + ">";
			String objectUri = "\"" + systemTMHash.get(system) + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>";
			
			insertQuery = insertQuery + subjectUri + " " + predUri + " " + objectUri + ". ";
		}
		insertQuery = insertQuery + "}";
		
		return insertQuery;
	}
	
	/**
	 * This function returns the calculated Business Value of the systems based on an input set of capabilities.
	 * 
	 * @param capabilities Set<String> of the capabilities used
	 * @return businessValues HashMap<String,Double> containing the values for all the systems.
	 * @throws Exception
	 */
	public HashMap<String, Double> calculateBusinessFunction(Set<String> capabilities) {
				
		String tapCoreData = "TAP_Core_Data";
		String tapSiteData = "TAP_Site_Data";
		String systemQuery = "SELECT DISTINCT ?system (\"Sum\" as ?Sum) (0 as ?Value) WHERE {{?system a <http://semoss.org/ontologies/Concept/System>}}";
		String bpActivityQuery = "SELECT DISTINCT ?BusinessProcess ?Activity ?Aw WHERE{{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>}{?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> }{?BusinessProcess ?consists ?Activity}{?consists <http://semoss.org/ontologies/Relation/Contains/weight> ?Aw}}";
		String mtfFCCQuery = "SELECT DISTINCT ?MTF ?FCC ?TC WHERE{{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?FCC_MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC-MTF>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?FCC <http://semoss.org/ontologies/Relation/Has> ?FCC_MTF}{?FCC_MTF <http://semoss.org/ontologies/Relation/Occurs_At> ?MTF}{?FCC_MTF <http://semoss.org/ontologies/Relation/Contains/TotalCost> ?TC}}";
		String bpFCCQuery = "SELECT DISTINCT ?BusinessProcess ?FCC ?BPpb WHERE{{?FCC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FCC>}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>}{?PartOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/PartOf>} {?PartOf <http://semoss.org/ontologies/Relation/Contains/PercentBilled> ?BPpb}	{?FCC ?PartOf ?BusinessProcess}}";
		String systemMTFMatrixQuery = "SELECT DISTINCT ?System ?MTF ?W WHERE{{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>}{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite}{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite} {?DCSite <http://semoss.org/ontologies/Relation/Includes> ?MTF}BIND(1 as ?W )}";
		
		MatrixHashMap systemList = new MatrixHashMap(QueryProcessor.getDoubleMap(systemQuery, tapCoreData));
		
		//Calculate System to Activity Matrix
		MatrixHashMap systemActivityMatrix = calculateSystemActivityMatrix().transposeMatrix();
		
		//Queries
		HashMap<String, HashMap<String,Double>> bpActivityHash = QueryProcessor.getDoubleMap(bpActivityQuery, tapCoreData);
		HashMap<String, HashMap<String, Double>> mtfFCCHash = QueryProcessor.getDoubleMap(mtfFCCQuery, tapSiteData);
		HashMap<String, HashMap<String, Double>> bpFCCHash = QueryProcessor.getDoubleMap(bpFCCQuery, tapCoreData);

		//Matrix Set Up (Queries)
		MatrixHashMap activityBPMatrix = new MatrixHashMap(bpActivityHash).transposeMatrix();
		MatrixHashMap fccMTFMatrix = new MatrixHashMap(mtfFCCHash).transposeMatrix();
		MatrixHashMap bpFCCMatrix = new MatrixHashMap(bpFCCHash);
		MatrixHashMap systemMTFMask = new MatrixHashMap(QueryProcessor.getDoubleMap(systemMTFMatrixQuery, tapSiteData));

		//Math
		MatrixHashMap systemBPMatrix = systemActivityMatrix.multiplyMatrix(activityBPMatrix);
		Set<String> bpMask = getBPMask(capabilities);
		MatrixHashMap bpMTFMatrix = bpFCCMatrix.multiplyMatrix(fccMTFMatrix).maskRows(bpMask);
		Double total = bpMTFMatrix.getSum();
		MatrixHashMap systemMTFMatrix = systemBPMatrix.multiplyMatrix(bpMTFMatrix);	
		MatrixHashMap systemBV = systemMTFMatrix.maskMatrix(systemMTFMask).getRowSum().multiplyByScalar(1/total);

		// Add systemList matrix to ensure all systems are present in the returned hashmap
		return systemBV.addMatrix(systemList).getColumn("Sum");
		
	}

	/**
	 * Calculates Business Value base on using all capabilities
	 * 
	 * @return businessValues HashMap<String,Double> containing the values for all the systems.
	 */
	public HashMap<String, Double> calculateBusinessFunction() {
		//Get all capabilities
		String capabilitiesQuery = "SELECT DISTINCT ?Capability WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} }";
		ArrayList<String> capabilitiesList = QueryProcessor.getStringList(capabilitiesQuery, "TAP_Core_Data");
		Set<String> capabilities = new HashSet<>(capabilitiesList);
		return calculateBusinessFunction(capabilities);
	}
	
	/**
	 * Returns the a Set of Strings that list the Business Process that should be evaluated based
	 *  on the input capability Set<String>.
	 *  
	 * @param Capabilities Set<String>
	 * @return BusinessProcesses Set<String>
	 */
	public Set<String> getBPMask(Set<String> Capabilities) {
		String bpCapabilitiesQuery = "SELECT DISTINCT ?BP ?Capability ?W WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?BP <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?Capability  <http://semoss.org/ontologies/Relation/Supports> ?BP} Bind(1 as ?W) }";
		//ArrayList<String> capabilitiesList = QueryProcessor.getStringList(capabilitiesQuery, "TAP_Core_Data");
		MatrixHashMap bpCapabilitesMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(bpCapabilitiesQuery, "TAP_Core_Data"));
		return bpCapabilitesMatrix.getRowMask(Capabilities);
	}
	
	/**
	 * Sets the SOA Alpha value.
	 * @param value 	Alpha value, in double form.
	 */
	public void setsoaAlphaValue(double value){
		soaAlphaValue = value;
	}
	
	/**
	 * This function calculates the System Activity matrix that is necessary for the business value.
	 * 
	 * @return activitySystem  MatrixHashMap that contains the calculated System to Activity Matrix
	 * @throws Exception
	 */
	private MatrixHashMap calculateSystemActivityMatrix() {
		
		String tapCoreData = "TAP_Core_Data";
		//Declare Queries for matrices
		String activityDataQuery = "SELECT ?activity1 ?data ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?activity1 ?need ?data ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1} }";
		String dataSystemAlphaQuery = "SELECT ?data ?system (?weight2*@soaAlphaValue@ AS ?Weight2) WHERE{ {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/weight>  ?weight2}}";
		String dataSystemBetaQuery = "SELECT ?data ?system (?NETWORKweight*@networkBetaValue@ AS ?NETWORKWeight) WHERE{ {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/NetworkWeight> ?NETWORKweight} }";		
		String activityBluQuery = "SELECT ?activity1 ?blu ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?activity1 ?need ?blu ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1} }";
		String activitySplitQuery = "SELECT ?act (IF(?rel=<http://semoss.org/ontologies/Relation/Contains/Dataweight>,<http://semoss.org/ontologies/Concept/Element/Data>,<http://semoss.org/ontologies/Concept/Element/BLU>) AS ?element) ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?act ?rel ?weight1} }BINDINGS ?rel { (<http://semoss.org/ontologies/Relation/Contains/Dataweight>)(<http://semoss.org/ontologies/Relation/Contains/BLUweight>) }";
		String activityFErrorQuery = "SELECT ?act (1 - SUM(?weight1) as ?weight) WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;} {?ferror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError> ;} {?act ?have ?ferror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1} }GROUP BY ?act";
		String bluSystemQuery = "SELECT ?blu ?system ?weight2 WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?blu ;} {?provide <http://semoss.org/ontologies/Relation/Contains/weight> ?weight2} }";
		
		//Generate Matrices for calculation
		MatrixHashMap activityDataMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(activityDataQuery, tapCoreData));
		MatrixHashMap dataSystemAlphaMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(dataSystemAlphaQuery.replace("@soaAlphaValue@", Double.toString(soaAlphaValue)), tapCoreData));
		MatrixHashMap dataSystemBetaMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(dataSystemBetaQuery.replace("@networkBetaValue@", Double.toString(1.0-soaAlphaValue)), tapCoreData));
		MatrixHashMap activityBluMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(activityBluQuery, tapCoreData));
		MatrixHashMap activitySplitMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(activitySplitQuery, tapCoreData));
		HashMap<String,Double> activityFErrorVector = QueryProcessor.getDoubleVector(activityFErrorQuery, tapCoreData);
		MatrixHashMap bluSystemMatrix = new MatrixHashMap(QueryProcessor.getDoubleMap(bluSystemQuery, tapCoreData));
		
		//Begin Calculation of values
		MatrixHashMap dataSystemMatrix = dataSystemAlphaMatrix.addMatrix(dataSystemBetaMatrix);
		MatrixHashMap activitySystem_Data_Matrix = activityDataMatrix.multiplyMatrix(dataSystemMatrix);
		MatrixHashMap activitySystem_BLU_Matrix = activityBluMatrix.multiplyMatrix(bluSystemMatrix);

		//Apply Weights of Activity Split to BlU and Data Object
		HashMap<String,Double> activityBLUSplit = activitySplitMatrix.getColumn("BLU");
		HashMap<String,Double> activityDataSplit = activitySplitMatrix.getColumn("Data");		
		MatrixHashMap activitySystem_DataWeighted_Matrix = activitySystem_Data_Matrix.multiplyRowsByScalarVector(activityDataSplit);
		MatrixHashMap activitySystem_BLUWeighted_Matrix = activitySystem_BLU_Matrix.multiplyRowsByScalarVector(activityBLUSplit);
		//Sum BLU and Data contributions
		MatrixHashMap activitySystem = activitySystem_DataWeighted_Matrix.addMatrix(activitySystem_BLUWeighted_Matrix);

		//Scale by FError Vector
		activitySystem = activitySystem.multiplyRowsByScalarVector(activityFErrorVector);
		
		return activitySystem;
	}
}
