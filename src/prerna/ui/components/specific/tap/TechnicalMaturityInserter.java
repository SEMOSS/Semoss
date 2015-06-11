package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import prerna.engine.api.IEngine;
import prerna.ui.components.UpdateProcessor;
import prerna.util.DIHelper;

public class TechnicalMaturityInserter {
	/**
	 * Performs all calculations and insertions for each of the four Technical Maturities
	 */
	public void runCalculationsAndInsertions() {
		inserter(externalStabilityCalculation(), "ExternalStabilityTM");
		inserter(architecturalComplexityCalculation(), "ArchitecturalComplexityTM");
		inserter(informationAssuranceCalculation(), "InformationAssuranceTM");
		inserter(systemAvailabilityCalculation(), "NonFunctionalRequirementsTM");
	}
	
	/**
	 * Takes in System and property to be inserted as a HashMap and names the property according to propName
	 * 
	 * @param tmHash
	 *            HashMap with System name as key and Technical Maturity score as value
	 * @param propName
	 *            Name of property
	 */
	private void inserter(HashMap<String, Double> tmHash, String propName) {
		IEngine tapCoreData = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		
		// Loop through entire hashmap and create a giant insert query to be run by UpdateProcessor
		String insertQuery = prepareInsert(tmHash, propName);
		
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
	 * Performs calculation of External Stability Technical Maturity
	 * 
	 * @return HashMap with System as key and respective External Stability Technical Maturity Score as value
	 */
	private HashMap<String, Double> externalStabilityCalculation() {
		HashMap<String, Double> systemTMHash = new HashMap<String, Double>();
		String TAP_Core_Data = "TAP_Core_Data";
		String systemQuery = "SELECT DISTINCT ?system WHERE {{?system a <http://semoss.org/ontologies/Concept/System>}}";
		// System and its Software/Hardware
		String systemSoftwareHardwareQuery = "SELECT DISTINCT ?system ?softwareHardware WHERE {{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>}{?softwareHardware <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>}{?system ?has ?softwareHardware}} UNION {{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>} {?softwareHardware <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule>}{?system ?has ?softwareHardware }}}";
		// Software | EOL | Type
		String softwareParameterQuery = "SELECT DISTINCT ?softM (COALESCE(?category,'Platform') AS ?cat) (COALESCE(xsd:dateTime(?SoftMlifePot), COALESCE(xsd:dateTime(?SoftVlifePot),'TBD')) AS ?life) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?softM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} {?typeOf <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;} {?softV <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;}{?sys ?has ?softM ;} {?softM ?typeOf ?softV ;}OPTIONAL{{?category <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVCategory> ;}{?softV <http://semoss.org/ontologies/Relation/Has> ?category}}OPTIONAL{?softV <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftVlifePot ;}OPTIONAL{?softM <http://semoss.org/ontologies/Relation/Contains/EOL> ?SoftMlifePot ;}}";
		// Hardware | EOL | Type
		String hardwareParameterQuery = "SELECT DISTINCT ?Hardwarem ('Infrastructure' as ?type) (COALESCE(xsd:dateTime(?HardMlifePot), COALESCE(xsd:dateTime(?HardVlifePot),'TBD')) AS ?life) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}{?Hardwarem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareModule>;}{?System ?has ?Hardwarem.}{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TypeOf> ;}{?Hardwarev <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/HardwareVersion>;}{?Hardwarem ?typeof ?Hardwarev;}OPTIONAL{?Hardwarev <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardVlifePot ;}OPTIONAL{?Hardwarem <http://semoss.org/ontologies/Relation/Contains/EOL> ?HardMlifePot ;}}";
		
		ArrayList<String> systemList = QueryProcessor.getStringList(systemQuery, TAP_Core_Data);
		HashMap<String, ArrayList<String>> systemSoftwareHardwareHash = QueryProcessor.getStringListMap(systemSoftwareHardwareQuery, TAP_Core_Data);
		HashMap<String, String[]> softwareParameterHash = QueryProcessor.getStringTwoParameterMap(softwareParameterQuery, TAP_Core_Data);
		HashMap<String, String[]> hardwareParameterHash = QueryProcessor.getStringTwoParameterMap(hardwareParameterQuery, TAP_Core_Data);
		
		for (String system : systemList) {
			Double infrastructureScore = 0.0;
			int infrastructureCount = 0;
			Double platformScore = 0.0;
			int platformCount = 0;
			Double softwareScore = 0.0;
			int softwareCount = 0;
			Double finalTMScore = 0.0;

			if (systemSoftwareHardwareHash.keySet().contains(system)) {
				for (String softwareHardware : systemSoftwareHardwareHash.get(system)) {
					Double supportLevelScore = 0.0;
					Double typeWeight = 0.0;
					if (softwareParameterHash.containsKey(softwareHardware)) {
						String type = softwareParameterHash.get(softwareHardware)[0];
						typeWeight = getTypeWeight(type);
						supportLevelScore = getSupportLevelScore(softwareParameterHash.get(softwareHardware)[1]);
						if (type.equalsIgnoreCase("Infrastructure")) {
							infrastructureScore += typeWeight * supportLevelScore;
							infrastructureCount++;
						} else if (type.equalsIgnoreCase("Platform")) {
							platformScore += typeWeight * supportLevelScore;
							platformCount++;
						} else if (type.equalsIgnoreCase("Software")) {
							softwareScore += typeWeight * supportLevelScore;
							softwareCount++;
						}
					} else if (hardwareParameterHash.containsKey(softwareHardware)) {
						String type = hardwareParameterHash.get(softwareHardware)[0];
						typeWeight = getTypeWeight(type);
						supportLevelScore = getSupportLevelScore(hardwareParameterHash.get(softwareHardware)[1]);
						infrastructureScore += typeWeight * supportLevelScore; // Hardware is always infrastructure only
						infrastructureCount++;
					}
				}
			}
			if (infrastructureCount != 0) {
				infrastructureScore /= infrastructureCount;
			}
			if (platformCount != 0) {
				platformScore /= platformCount;
			}
			if (softwareCount != 0) {
				softwareScore /= softwareCount;
			}
			finalTMScore = infrastructureScore + platformScore + softwareScore;
			systemTMHash.put(system, finalTMScore);
		}
		
		return systemTMHash;
	}
	
	/**
	 * Takes in the Hardware/Software category and returns the weighted value equivalent to its respective category
	 * 
	 * @param type
	 *            String of Hardware/Software category
	 * @return Double of category weight. Infrastructure: 0.1, Platform: 0.3, Software: 0.6, Application: 0.0
	 */
	private Double getTypeWeight(String type) {
		if (type.equalsIgnoreCase("Infrastructure")) {
			return 0.1;
		} else if (type.equalsIgnoreCase("Platform")) {
			return 0.3;
		} else if (type.equalsIgnoreCase("Software")) {
			return 0.6;
		} else { // Application
			return 0.0;
		}
	}
	
	/**
	 * Takes in End of Life(EOL) date for Software/Hardware and assigns a score relative to how soon said date is compared to current date
	 * 
	 * @param date
	 *            String form of EOL date
	 * @return Score based on EOL date relative to current date.
	 */
	private Double getSupportLevelScore(String date) {
		if (date.length() < 8 || date.contains("TBD")) { // TBD or No Data (if String length is less than 8, date is in improper format)
			return 5.0;
		}
		// date = date.substring(1, date.substring(1).indexOf("\""));
		int eolYear = Integer.parseInt(date.substring(0, 4));
		int eolMonth = Integer.parseInt(date.substring(5, 7));
		
		Calendar now = Calendar.getInstance();
		int currentYear = now.get(Calendar.YEAR);
		int currentMonth = now.get(Calendar.MONTH) + 1;
		
		if ((eolYear < currentYear) || (eolYear == currentYear && eolMonth <= currentMonth + 6)
				|| (eolYear == currentYear + 1 && eolMonth <= currentMonth - 6)) { // Retired
			return 0.0;
		} else if (eolYear == currentYear || (eolYear == currentYear + 1 && eolMonth <= currentMonth)) { // Sunset
			return 5.0;
		} else if (eolYear <= currentYear + 2 || (eolYear == currentYear + 3 && eolMonth <= currentMonth)) { // Generally Available
			return 7.0;
		} else if (eolYear >= currentYear + 3 && eolMonth > currentMonth) { // Supported
			return 10.0;
		} else { // Default
			return 5.0;
		}
	}
	
	/**
	 * Performs calculation of Architectural Complexity Technical Maturity
	 * 
	 * @return HashMap with System as key and respective Architectural Complexity Technical Maturity Score as value
	 */
	private HashMap<String, Double> architecturalComplexityCalculation() {
		HashMap<String, Double> systemTMHash = new HashMap<String, Double>();
		String systemComplexityQuery = "SELECT DISTINCT ?system ?complexity WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}OPTIONAL{{?complexity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Complexity>}{?system <http://semoss.org/ontologies/Relation/Rated> ?complexity}}}";
		String TAP_Core_Data = "TAP_Core_Data";
		
		HashMap<String, String> systemComplexityHash = QueryProcessor.getStringMap(systemComplexityQuery, TAP_Core_Data);
		
		for (String system : systemComplexityHash.keySet()) {
			String complexity = systemComplexityHash.get(system);

			if (complexity.equalsIgnoreCase("Simple")) {
				systemTMHash.put(system, 10.0);
			} else if (complexity.equalsIgnoreCase("Complex")) {
				systemTMHash.put(system, 4.0);
			} else { // Medium and No data
				systemTMHash.put(system, 7.0);
			}
		}

		return systemTMHash;
	}
	
	/**
	 * Performs calculation of Information Assurance Technical Maturity
	 * 
	 * @return HashMap with System as key and respective Information Assurance Technical Maturity Score as value
	 */
	private HashMap<String, Double> informationAssuranceCalculation() { // TODO: Update once density data has been captured
		HashMap<String, Double> systemTMHash = new HashMap<String, Double>();
		String systemAccreditationQuery = "SELECT DISTINCT ?system (COALESCE(xsd:dateTime(?ato),'TBD') AS ?ATO) WHERE {{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} OPTIONAL{?system <http://semoss.org/ontologies/Relation/Contains/ATO_Date> ?ato ;}}";
		String TAP_Core_Data = "TAP_Core_Data";
		
		HashMap<String, String> systemAccreditationHash = QueryProcessor.getStringMap(systemAccreditationQuery, TAP_Core_Data);
		
		for (String system : systemAccreditationHash.keySet()) {
			String date = systemAccreditationHash.get(system);
			
			if (date.length() < 8 || date.contains("TBD")) { // TBD or No Data (if String length is less than 8, date is in improper format)
				systemTMHash.put(system, 5.0);
				continue;
			}
			
			// date = date.substring(1, date.substring(1).indexOf("\""));
			int atoYear = Integer.parseInt(date.substring(0, 4)) + 3;
			int atoMonth = Integer.parseInt(date.substring(5, 7));
			
			Calendar now = Calendar.getInstance();
			int currentYear = now.get(Calendar.YEAR);
			int currentMonth = now.get(Calendar.MONTH) + 1;
			
			if ((atoYear < currentYear) || (atoYear == currentYear && atoMonth <= currentMonth + 6)
					|| (atoYear == currentYear + 1 && atoMonth <= currentMonth - 6)) { // Retired
				systemTMHash.put(system, 0.0);
			} else if (atoYear == currentYear || (atoYear == currentYear + 1 && atoMonth <= currentMonth)) { // Sunset
				systemTMHash.put(system, 5.0);
			} else if (atoYear <= currentYear + 2 || (atoYear == currentYear + 3 && atoMonth <= currentMonth)) { // Generally Available
				systemTMHash.put(system, 10.0);
			} else { // Default
				systemTMHash.put(system, 5.0);
			}
		}
		
		return systemTMHash;
	}
	
	/**
	 * Performs calculation of Non-Functional Requirements Technical Maturity (named systemAvailabilityCalculation because it bases score off system's
	 * "Availability-Actual")
	 * 
	 * @return HashMap with System as key and respective Non-Functional Requirements Technical Maturity Score as value
	 */
	private HashMap<String, Double> systemAvailabilityCalculation() {
		HashMap<String, Double> systemTMHash = new HashMap<String, Double>();
		String systemAvailabilityQuery = "SELECT DISTINCT ?system (COALESCE(?availability,'NA') AS ?Availability) WHERE {{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} OPTIONAL{?system <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?availability ;}}";
		String availabilitySumQuery = "SELECT DISTINCT (SUM(COALESCE(xsd:decimal(?availability),0.0)) AS ?Average) WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?system <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?availability ;}}";
		String systemCountQuery = "SELECT DISTINCT (COUNT(?A) AS ?Average) WHERE{ SELECT (xsd:decimal(?availability) as ?A) WHERE{{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?system <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?availability ;}}}";
		String TAP_Core_Data = "TAP_Core_Data";
		
		HashMap<String, String> systemAvailabilityHash = QueryProcessor.getStringMap(systemAvailabilityQuery, TAP_Core_Data);
		Double availabilitySum = QueryProcessor.getSingleCount(availabilitySumQuery, TAP_Core_Data);
		Double systemCount = QueryProcessor.getSingleCount(systemCountQuery, TAP_Core_Data);
		Double scalingFactor = 5.0; // TODO: Expose this parameter for user to change
		Double averageScore = scalingFactor * availabilitySum / systemCount;
		
		for (String system : systemAvailabilityHash.keySet()) {
			String availability = systemAvailabilityHash.get(system);
			if (availability.equalsIgnoreCase("TBD") || availability.equalsIgnoreCase("NA")) {
				systemTMHash.put(system, averageScore);
				continue;
			}
			Double availabilityScore = Double.parseDouble(availability);
			systemTMHash.put(system, availabilityScore * 10);
		}
		
		return systemTMHash;
	}
	
	private void technicalStandardCalculation() {
		// TODO add standards calculation
	}

}