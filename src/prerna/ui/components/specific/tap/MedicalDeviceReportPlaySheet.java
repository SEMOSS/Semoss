package prerna.ui.components.specific.tap;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.specific.IndividualSystemTransitionReportWriter;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class MedicalDeviceReportPlaySheet extends TablePlaySheet{

	private static final Logger classLogger = LogManager.getLogger(MedicalDeviceReportPlaySheet.class);
	
	//Contains all of the queries

	//Query that draws determines if a system is a "low" system. Returns the system name and the disposition, which will be 'LPI' or 'LPNI'
	private static String LowSystemQuery = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?System <http://semoss.org/ontologies/Relation/Contains/Disposition> ?Disposition}{?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))  }  BINDINGS ?Disposition {('LPI')('LPNI')}";

	//draws all of the instances where the nameplate is already interfacing. Returns the manufacturer, nameplate, CAC, and system
	private static String InterfaceQuery = "SELECT DISTINCT ?Manufacturer (COALESCE(?Nameplate_Model, ?Common_Nomenclature) AS ?NP ) ?CAC ?System ?Nameplate  WHERE {{?Device <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Device>}{?Nameplate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nameplate>}{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Is_Medical_Device> 'Y'}{?Nameplate <http://semoss.org/ontologies/Relation/TypeOf> ?Device }OPTIONAL{{?CAC <http://semoss.org/ontologies/Relation/Contains> ?Nameplate}{?CAC <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CernerApprovedCategory>}}OPTIONAL{{?Manufacturer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Manufacturer>}{?Manufacturer <http://semoss.org/ontologies/Relation/Produces> ?Nameplate}}OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Nameplate_Model> ?Nameplate_Model }OPTIONAL{?Device <http://semoss.org/ontologies/Relation/Contains/ECN> ?ECN }OPTIONAL{{?Common_Nomenclature <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Common_Nomenclature>}{?Common_Nomenclature <http://semoss.org/ontologies/Relation/Consists> ?Device}}{SELECT DISTINCT ?Device ?System ?DCSite WHERE{{?Device <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Device>}{?SystemInterface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>}{?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>}{{?Device <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface }{?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?SystemDCSite }}UNION{{?SystemInterface <http://semoss.org/ontologies/Relation/Consume> ?Device }{?SystemDCSite <http://semoss.org/ontologies/Relation/Provide> ?SystemInterface }}{?SystemDCSite <http://semoss.org/ontologies/Relation/DeployedAt> ?DCSite }{?System <http://semoss.org/ontologies/Relation/DeployedAt> ?SystemDCSite }}}}ORDER BY ?Manufacturer ?Nameplate_Model";           

	//Pulls all of the instances where the nameplate is on the cerner approved list. Returns the manufacturer, nameplate, and cernerapprovedcategory
	private static String CernerApprovedQuery = "SELECT DISTINCT ?Manufacturer ?Nameplate_Model ?Nameplate ?CernerApprovedCategory WHERE { {?Manufacturer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Manufacturer>} {?Nameplate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nameplate>} {?CernerApprovedCategory <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CernerApprovedCategory>} {?Manufacturer <http://semoss.org/ontologies/Relation/Produces> ?Nameplate} {?CernerApprovedCategory <http://semoss.org/ontologies/Relation/Contains> ?Nameplate} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Nameplate_Model> ?Nameplate_Model } {?Device <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Device>} {?Nameplate <http://semoss.org/ontologies/Relation/TypeOf> ?Device } }";

	//Pull all of the instances where the requirements for being RTM is met. Returns the manufacturer, nameplate, RTM aspects and if it is "Imagining and Radiology"
	public static String RTMMandatedQuery = "SELECT DISTINCT ?Manufacturer ?Nameplate (COALESCE(?Nameplate_Model, ?Common_Nomenclature) AS ?NP ) ?Point_of_Care_Lab_Device ?Bedside_Monitor ?Local_Specialty_Medical_Systems ?Lab_Instrument ?Imaging_And_Radiology ?RTM WHERE { {?Manufacturer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Manufacturer>} OPTIONAL{?Nameplate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nameplate>} {?Manufacturer <http://semoss.org/ontologies/Relation/Produces> ?Nameplate} {?Nameplate <http://semoss.org/ontologies/Relation/Contains/Is_Medical_Device> 'Y'} FILTER(\"Not in RTM\" != ?RTM ) OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Point_of_Care_Lab_Device> ?Point_of_Care_Lab_Device} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Bedside_Monitor> ?Bedside_Monitor} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Local_Specialty_Medical_Systems> ?Local_Specialty_Medical_Systems} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Lab_Instrument> ?Lab_Instrument} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Imaging_And_Radiology> ?Imaging_And_Radiology} {?Nameplate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nameplate>} {?Nameplate <http://semoss.org/ontologies/Relation/TypeOf> ?Device} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Nameplate_Model> ?Nameplate_Model } OPTIONAL{ {?Manufacturer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Manufacturer>} {?Manufacturer <http://semoss.org/ontologies/Relation/Produces> ?Nameplate} } BIND(IF(!BOUND(?Point_of_Care_Lab_Device) && !BOUND(?Bedside_Monitor) && !BOUND(?Local_Specialty_Medical_Systems) && !BOUND(?Lab_Instrument), \"Not in RTM\", \"In RTM\") as ?RTM) OPTIONAL{ {?Common_Nomenclature <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Common_Nomenclature>} {?Common_Nomenclature <http://semoss.org/ontologies/Relation/Consists> ?Device} } }";

	//Pulls all of the MTFs that a certain nameplate is used at
	public static String MTFQuery = "SELECT DISTINCT ?Manufacturer ?Nameplate ?Device ?MTF ?Nameplate__Nameplate_Model (COALESCE(?Nameplate__Nameplate_Model, ?Common_Nomenclature) AS ?NP ) WHERE { {?Nameplate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nameplate>} {?Device <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Device>} {?MTF <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MTF>} OPTIONAL{{?Manufacturer <http://semoss.org/ontologies/Relation/Produces> ?Nameplate} {?Manufacturer <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Manufacturer>} } {?Nameplate <http://semoss.org/ontologies/Relation/TypeOf> ?Device} {?MTF <http://semoss.org/ontologies/Relation/Owns> ?Device} OPTIONAL{?Nameplate <http://semoss.org/ontologies/Relation/Contains/Nameplate_Model> ?Nameplate__Nameplate_Model} OPTIONAL{{?Common_Nomenclature <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Common_Nomenclature>}{?Common_Nomenclature <http://semoss.org/ontologies/Relation/Consists> ?Device}} }";


	//Declaring Variables for taking in and processing queries
	private final String SYS_URI_PREFIX = "http://health.mil/ontologies/Concept/System/";
	private String systemURI = "";
	private String systemName = "";
	private String reportType = "";
	private boolean showMessages = true;

	//Creates the HashMaps that will be used to store the separate lists
	ArrayList <String> lowSystemList = new ArrayList<String>();
	HashMap <String, ArrayList<String>> interfacesList = new HashMap<String, ArrayList<String>>();
	HashMap <String, ArrayList<String>> GALListPart1 =  new HashMap<String,ArrayList<String>>();
	HashMap <String, ArrayList<String>> GALListPart2 =  new HashMap<String,ArrayList<String>>();
	HashMap <String, ArrayList<String>> GALListPart3 =  new HashMap<String,ArrayList<String>>();
	HashMap <String, ArrayList<String>> InterfacingWithLowList =  new HashMap<String,ArrayList<String>>();
	HashMap <String, ArrayList<String>> GALListFinal =  new HashMap<String,ArrayList<String>>();
	HashMap <String, ArrayList<String>> MTFList =  new HashMap<String,ArrayList<String>>();
	ArrayList<String> MTFUniqueList = new ArrayList <String>();


	//Main method for connecting to the engines and running the methods
	public void createData() {

		//instantiates the engines to 
		try{
			IDatabaseEngine tapCore = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			if(tapCore==null)
				throw new NullPointerException();
		} catch(RuntimeException e) {
			Utility.showError("Could not find necessary database: TAP_Core_Data. Cannot generate report.");
			return;
		}

		try{
			IDatabaseEngine tapDevice = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Device_Data");
			if(tapDevice==null)
				throw new NullPointerException();
		} catch(RuntimeException e) {
			Utility.showError("Could not find necessary database: TAP_Device_Data. Cannot generate report.");
			return;
		}

		
		//runs all of the methods that have been developed
		processQueryParts(query);
		specifySysInQueriesForReport();
		createLowSystemList();
		createInterfaceList();
		createCernerApprovedList();
		createLowAndInterfaceLists();
		createRTMList();
		createMTFList();
		createGALListFinal();

		//create boolean to check if the writing the report worked
		boolean success = false;
		try {
			success = writeReport();
		} catch (IOException e) {

			classLogger.error(Constants.STACKTRACE, e);
		}

		//gives an output in SEMOSS to show that the output worked and shows the location.
		if(showMessages)
		{
			if(success){
				Utility.showMessage("System Export Finished! File located in:\n" + IndividualSystemTransitionReportWriter.getFileLoc() );
			} else {
				Utility.showError("Error Creating Report!");
			}
		}
	}



	private void processQueryParts(String query) {
		String[] systemAndType = this.query.split("\\$");
		systemURI = systemAndType[0];
		reportType = systemAndType[1];
		systemName = systemURI.substring(systemURI.lastIndexOf("/")+1,systemURI.lastIndexOf(">"));
	}

	private void specifySysInQueriesForReport() 
	{		
		if(systemURI.equals("")){
			processQueryParts(this.query);
		}
		LowSystemQuery = LowSystemQuery.replace("@SYSTEM@", systemURI);
		InterfaceQuery = InterfaceQuery.replace("@SYSTEM@", systemURI);
		CernerApprovedQuery = CernerApprovedQuery.replace("@SYSTEM@", systemURI);
		RTMMandatedQuery = RTMMandatedQuery.replace("@SYSTEM@", systemURI);
		MTFQuery = MTFQuery.replace("@SYSTEM@", systemURI);
	}

	//Method that runs the query for determining the 'low' probability systems and returns them to a HashMap. Uses the tap core engine to do so.
	public void createLowSystemList(){

		//              ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(tapCore, LowSystemQuery );
		//              String[] row = wrapper.getVariables();   
		//    	   
		//              while(wrapper.hasNext()) {
		//                     ISelectStatement ss = wrapper.next();
		//                     String systemName = (String) ss.getVar(row[0].toString());
		//                     String systemProbability = (String) ss.getVar(row[1].toString());
		//
		//                     lowSystemList.put(systemName, systemProbability);
		//
		//              }

		lowSystemList = QueryProcessor.getStringList(LowSystemQuery, "TAP_Core_Data");
	}

	//Method that runs the query for determining the interfacing nameplates and returns them to a HashMap. Uses the tap device engine to do so.
	public void createInterfaceList() {

		IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Device_Data");
		ISelectWrapper wrapper = Utility.processQuery(engine, InterfaceQuery);
		String[] row = wrapper.getVariables(); 	   

		//              ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(tapDevice, InterfaceQuery );
		//              String[] row = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();

			//this section takes the specific types of information out of the query
			String manufacturer = (String) ss.getVar(row[0].toString());

			//nameplateModel is different because it has to deal with doubles and strings
			String nameplateModel = "";
			if( ss.getVar(row[1].toString()) instanceof String)
			{nameplateModel = (String) ss.getVar(row[1].toString());}
			else if( ss.getVar(row[1].toString()) instanceof Double)
			{nameplateModel = Double.toString((Double)ss.getVar(row[1].toString()));}

			//String CAC = (String) row[2];
			String systemName = (String) ss.getVar(row[3].toString());
			String nameplate = nameplateModel + "%" + manufacturer;

			//checks if the interfacesList already has the nameplate, then adds a system if it does and everything if it doesnt. Does this becasue some nameplates support more than 1 system.
			if( interfacesList.containsKey(nameplate))
			{
				ArrayList<String> informationAboutNameplate = interfacesList.get(nameplate);
				informationAboutNameplate.add(systemName); 
				interfacesList.put(nameplate, informationAboutNameplate);
			}
			else
			{
				ArrayList<String> informationAboutNameplate = new ArrayList<String>();
				informationAboutNameplate.add(manufacturer);
				informationAboutNameplate.add(nameplateModel);
				informationAboutNameplate.add(systemName); 
				interfacesList.put(nameplate, informationAboutNameplate);
			}

		}
	}

	//Method that compares the 'low' probability list and the interfaces list. If the interfacing nameplate's system is not 'low', it is added to GALList1 
	public void createLowAndInterfaceLists() {

		for( String nameplate : interfacesList.keySet()) {
			ArrayList<String> informationAboutNameplate = interfacesList.get(nameplate);
			for( int i = 2; i < informationAboutNameplate.size(); i++)
			{
				String system = informationAboutNameplate.get(i);

				//checks if it is a low system, if not then it adds the info to GALListPart1
				if(lowSystemList.contains(system))
				{
					InterfacingWithLowList.put(nameplate, informationAboutNameplate);				}
				else{
					GALListPart1.put(nameplate, informationAboutNameplate);
				}
			}  
		}
	}

	//Method that runs the query for determining the Cerner Approved nameplates and returns them to a HashMap. Uses the tap device engine to do so.
	public void createCernerApprovedList(){


		IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Device_Data");
		ISelectWrapper wrapper = Utility.processQuery(engine, CernerApprovedQuery);
		String[] row = wrapper.getVariables();

		//    	   ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(tapDevice, CernerApprovedQuery );
		//    	   String[] row = wrapper.getVariables();   
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String manufacturer = (String) ss.getVar(row[0].toString());
			String nameplateModel = (String) ss.getVar(row[1].toString());
			//here, nameplate can be pulled and not created. The cerner list always has the correct nameplate
			String nameplate = (String) ss.getVar(row[2].toString());
			String cernerApprovedCategory = (String) ss.getVar(row[3].toString());

			//creates an arraylist to hold the values besides nameplate, to be added to the GALListPart3
			ArrayList<String> informationAboutNameplate = new ArrayList<String>();
			informationAboutNameplate.add(manufacturer);
			informationAboutNameplate.add(nameplateModel);
			informationAboutNameplate.add(cernerApprovedCategory);

			GALListPart2.put(nameplate, informationAboutNameplate);
		}
	}

	//Method that runs the query for determining the RTM required nameplates and returns them to a HashMap. Uses the tap device engine to do so.
	public void createRTMList(){

		IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Device_Data");
		ISelectWrapper wrapper = Utility.processQuery(engine, RTMMandatedQuery);
		String[] row = wrapper.getVariables();

		//    	   ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(tapDevice, RTMMandatedQuery );
		//    	   String[] row = wrapper.getVariables();   
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();

			//gets each column from the wrapper and sets it as a value
			String manufacturer = (String) ss.getVar(row[0].toString());

			//nameplateModel is different because it deals with incoming doubles and strings. Does the same thing in "createInterfaceList"
			String nameplateModel = "";
			if( ss.getVar(row[2].toString()) instanceof String)
			{nameplateModel = (String) ss.getVar(row[2].toString());}
			else if( ss.getVar(row[2].toString()) instanceof Double)
			{nameplateModel = Double.toString((Double)ss.getVar(row[2].toString()));}

			String nameplate = nameplateModel + "%" + manufacturer;
			String pointOfCareLabDevice = (String) ss.getVar(row[3].toString());
			String bedsideMonitor = (String) ss.getVar(row[4].toString());
			String localSpecialyMedicalSystem = (String) ss.getVar(row[5].toString());
			String labInstrument = (String) ss.getVar(row[6].toString());
			String imagingAndRadiology = (String) ss.getVar(row[7].toString());

			//creates an arraylist to hold the values besides nameplate, to be added to the GALListPart3
			ArrayList<String> informationAboutNameplate = new ArrayList<String>();
			informationAboutNameplate.add(manufacturer);
			informationAboutNameplate.add(nameplateModel);
			informationAboutNameplate.add(labInstrument);
			informationAboutNameplate.add(pointOfCareLabDevice);
			informationAboutNameplate.add(bedsideMonitor);
			informationAboutNameplate.add(localSpecialyMedicalSystem);
			//informationAboutNameplate.add(imagingAndRadiology);
			if(!InterfacingWithLowList.keySet().contains(nameplate))
				GALListPart3.put(nameplate, informationAboutNameplate);

		}
	}

	//Creates the final GAL List of nameplates, manufactures and nameplate models
	public void createGALListFinal() {

		//a local hashmap to store the values so that the specific MTF locations can be designated
		HashMap <String, ArrayList<String>> GALListFinalIntermediary =  new HashMap<String,ArrayList<String>>();

		//adds the manufacturer, nameplate Model and MTFs from each of the 3 lists to the final list
		for (String key: GALListPart1.keySet()){
			ArrayList <String> infoToEnter = new ArrayList <String>();

			//adds the manufacturer, nameplate, and MTFs to the intermediary list
			infoToEnter.add(GALListPart1.get(key).get(0));
			infoToEnter.add(GALListPart1.get(key).get(1)); 
			ArrayList<String> List = (ArrayList<String>) MTFList.get(key);
			infoToEnter.addAll(List);
			GALListFinalIntermediary.put(key, infoToEnter);
		} 

		for (String key: GALListPart2.keySet()){;
		ArrayList <String> infoToEnter = new ArrayList <String>();

		//adds the manufacturer, nameplate, and MTFs to the intermediary list
		infoToEnter.add(GALListPart2.get(key).get(0));
		infoToEnter.add(GALListPart2.get(key).get(1));
		infoToEnter.addAll(MTFList.get(key));
		GALListFinalIntermediary.put(key, infoToEnter);  
		} 

		for (String key: GALListPart3.keySet()){
			ArrayList <String> infoToEnter = new ArrayList <String>();

			//adds the manufacturer, nameplate, and MTFs to the intermediary list
			infoToEnter.add(GALListPart3.get(key).get(0));
			infoToEnter.add(GALListPart3.get(key).get(1));
			infoToEnter.addAll(MTFList.get(key));
			GALListFinalIntermediary.put(key, infoToEnter);  
		} 

		//puts the MFTs in the correct location
		for (String key: GALListFinalIntermediary.keySet()) {

			ArrayList <String> nameplateInfo = GALListFinalIntermediary.get(key);
			ArrayList <String> infoForFinal = new ArrayList <String>();

			//extracts the manufacturer and nameplateModel from the intermediary list. Then adds them to the arraylist for the final
			String manufacturer = nameplateInfo.get(0);
			String nameplateModel = nameplateInfo.get(1);
			infoForFinal.add(manufacturer);
			infoForFinal.add(nameplateModel);

			//adds a spot for the index so that the index can be changed to a 'Y' if it contains the MTF
			for ( int i = 1; i <= MTFUniqueList.size(); i++) {
				infoForFinal.add("");
			}

			//needs to add something to catch exception if the MTFunique list size changes
			//Goes through and designates a spot in the infoForFinal arraylist if a specific MTF is contained.
			if (nameplateInfo.contains("141st_MDG")) {
				infoForFinal.set(2, "Y");
			}
			if (nameplateInfo.contains("92nd_MEDICAL_GROUP")) {
				infoForFinal.set(3, "Y");
			}
			if (nameplateInfo.contains("MADIGAN_AMC")) {
				infoForFinal.set(4, "Y");
			}
			if (nameplateInfo.contains("NBHC_PUGET_SOUND")) {
				infoForFinal.set(5, "Y");
			}
			if (nameplateInfo.contains("NBHC_SUBASE_BANGOR")) {
				infoForFinal.set(6, "Y");
			}
			if (nameplateInfo.contains("NH_BREMERTON")) {
				infoForFinal.set(7, "Y");
			}
			if (nameplateInfo.contains("NH_OAK_HARBOR")) {
				infoForFinal.set(8, "Y");
			}

			if (nameplateInfo.contains("NHCL_EVERETT")) {
				infoForFinal.set(9, "Y");
			}

			GALListFinal.put(key, infoForFinal);
		}
	}

	public void createMTFList() {

		IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Device_Data");
		ISelectWrapper wrapper = Utility.processQuery(engine, MTFQuery);
		String[] row = wrapper.getVariables();

		//    	   ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(tapDevice, MTFQuery );
		//    	   String[] row = wrapper.getVariables();   
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();

			//gets each column from the wrapper and sets it as a value
			String manufacturer = (String) ss.getVar(row[0].toString());

			String nameplateModel = ""; 
			if( ss.getVar(row[5].toString()) instanceof String)
			{nameplateModel = (String) ss.getVar(row[5].toString());}
			else if( ss.getVar(row[5].toString()) instanceof Double)
			{nameplateModel = Double.toString((Double)ss.getVar(row[5].toString()));}

			String nameplate = nameplateModel + "%" + manufacturer;
			String MTF = (String) ss.getVar(row[3].toString());

			//Adds MTFs to the main MTFs. Checks if the list has a nameplate, then if that nameplate already is marked for a specific MTF.
			if (MTFList.containsKey(nameplate) ) {

				if (MTFList.get(nameplate).contains(MTF)) {

				} 
				else {
					MTFList.get(nameplate).add(MTF);
				}
			}
			else {
				MTFList.put(nameplate, new ArrayList <String>());
				MTFList.get(nameplate).add(MTF);
			}

			//Creates the unique list of all of the MTFs, then sorts it alphabetically
			if (MTFUniqueList.contains(MTF)) {

			} 
			else {
				MTFUniqueList.add(MTF);
			}
		}  
		Collections.sort(MTFUniqueList);
	} 

	//creates and writes the report to excel. Uses the class InvidualSystemTransitionReportWriter to do so
	private boolean writeReport() throws IOException {

		IndividualSystemTransitionReportWriter writer = new IndividualSystemTransitionReportWriter();
		String templateFileName = "";
		templateFileName = "Medical_Devices_GAL_Report.xlsx";

		writer.makeMedicalDevicesWorkbook(templateFileName);
		writer.writeArrayListSheet("Master List", GALListFinal);
		writer.writeArrayListSheet("Cat 1", GALListPart1);
		writer.writeArrayListSheet("Cat 2", GALListPart2);
		writer.writeArrayListSheet("Cat 3", GALListPart3);

		writer.writeWorkbook();
		return true;
	}


}
