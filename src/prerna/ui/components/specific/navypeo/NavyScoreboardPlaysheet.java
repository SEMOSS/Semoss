package prerna.ui.components.specific.navypeo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.BrowserPlaySheet;


public class NavyScoreboardPlaysheet extends BrowserPlaySheet {

	//Query for getting details about a system
//	private static String systemDetailQuery = "SELECT DISTINCT ?System ?System_Name ?Description ?DITPR_DON_ID ?FAM ?Command ?MA_Domain ?Resource_Sponsor ?MAC_Level ?CSRG_Level ?ATO_Expiration ?Overall_Assessment WHERE{	"
//			+ "{?System a <http://semoss.org/ontologies/Concept/System>}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/Contains/System_Name> ?System_Name}	"
//			+ "OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/Contains/DITPR-DON_ID> ?DITPR_DON_ID}	"
//			+ "OPTIONAL{{?FAM a <http://semoss.org/ontologies/Concept/FAM>}	"
//			+ "{?FAM <http://semoss.org/ontologies/Relation/Categorizes> ?System}}	"
//			+ "OPTIONAL{{?Command a <http://semoss.org/ontologies/Concept/Command>}	"
//			+ "{?Command <http://semoss.org/ontologies/Relation/Dictates> ?System}}	"
//			+ "OPTIONAL{{?MA_Domain a <http://semoss.org/ontologies/Concept/MA_Domain>}	"
//			+ "{?MA_Domain <http://semoss.org/ontologies/Relation/Contains> ?System}}	"
//			+ "OPTIONAL{{?Resource_Sponsor a <http://semoss.org/ontologies/Concept/Resource_Sponsor>}	"
//			+ "{?Resource_Sponsor <http://semoss.org/ontologies/Relation/Funds> ?System}}	"
//			+ "OPTIONAL{{?MAC_Level a <http://semoss.org/ontologies/Concept/MAC_Level>}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/ClassifiedAs> ?MAC_Level}}	"
//			+ "OPTIONAL{{?CSRG_Level a <http://semoss.org/ontologies/Concept/CSRG_Level>}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/HasCSRG> ?CSRG_Level}}		"
//			+ "{?Overall_Assessment a <http://semoss.org/ontologies/Concept/CRA_Assessment>}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/AssessedOverall> ?Overall_Assessment}		"
//			+ "BIND(<@System@> AS ?System)}";
	
	private static String systemDetailQuery = "SELECT DISTINCT ?System ?System_Name ?Description ?DITPR_DON_ID ?FAM ?Command ?MA_Domain ?Resource_Sponsor ?MAC_Level ?CSRG_Level ?Overall_Assessment WHERE{	{?System a <http://semoss.org/ontologies/Concept/System>}	{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}	{?System <http://semoss.org/ontologies/Relation/Contains/System_Name> ?System_Name}	OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> ?NeedsAssessment}	OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}	{?System <http://semoss.org/ontologies/Relation/Contains/DITPR-DON_ID> ?DITPR_DON_ID}	OPTIONAL{{?FAM a <http://semoss.org/ontologies/Concept/FAM>}	{?FAM <http://semoss.org/ontologies/Relation/Categorizes> ?System}}	OPTIONAL{{?Command a <http://semoss.org/ontologies/Concept/Command>}	{?Command <http://semoss.org/ontologies/Relation/Dictates> ?System}}	OPTIONAL{{?MA_Domain a <http://semoss.org/ontologies/Concept/MA_Domain>}	{?MA_Domain <http://semoss.org/ontologies/Relation/Contains> ?System}}	OPTIONAL{{?Resource_Sponsor a <http://semoss.org/ontologies/Concept/Resource_Sponsor>}	{?Resource_Sponsor <http://semoss.org/ontologies/Relation/Funds> ?System}}	OPTIONAL{{?MAC_Level a <http://semoss.org/ontologies/Concept/MAC_Level>}	{?System <http://semoss.org/ontologies/Relation/ClassifiedAs> ?MAC_Level}}	OPTIONAL{{?CSRG_Level a <http://semoss.org/ontologies/Concept/CSRG_Level>}	{?System <http://semoss.org/ontologies/Relation/HasCSRG> ?CSRG_Level}}		OPTIONAL {?System <http://semoss.org/ontologies/Relation/Contains/Accreditation_Expiration> ?Accreditation_Expiration}		OPTIONAL{{?Overall_Assessment a <http://semoss.org/ontologies/Concept/CRA_Assessment>}	{?System <http://semoss.org/ontologies/Relation/AssessedOverall> ?Overall_Assessment}}	BIND(<@System@> AS ?System)}";
	
	//Query for getting information from blank category systems
//	private static String blankCategoryQuery = "SELECT DISTINCT ?System ?CRA_Archtype ?CRA_Category ?Prefix ?Value ?Suffix ?CRA_Assessment ((?Rank)*(?Assessment) AS ?Score) WHERE{		"
//			+ "{?System a <http://semoss.org/ontologies/Concept/System>}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/HasCRA> ?CRA_Value}	"
//			+ "{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}	"
//			+ "{?CRA_Archtype a <http://semoss.org/ontologies/Concept/CRA_Archtype>}	"
//			+ "{?CRA_Archtype <http://semoss.org/ontologies/Relation/Contains> ?CRA_Category}		"
//			+ "{?CRA_Value a <http://semoss.org/ontologies/Concept/CRA_Value>}	"
//			+ "{?CRA_Assessment a <http://semoss.org/ontologies/Concept/CRA_Assessment>}	"
//			+ "{?CRA_Value <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_Assessment}	"
//			+ "{?CRA_Assessment <http://semoss.org/ontologies/Relation/Contains/Assessment_Value> ?Assessment}	"
//			+ "{?CRA_Category a <http://semoss.org/ontologies/Concept/CRA_Category>}	"
//			+ "{?CRA_Category <http://semoss.org/ontologies/Relation/Contain> ?CRA_Value}	"
//			+ "{?CRA_Rank a <http://semoss.org/ontologies/Concept/CRA_Rank>}	"
//			+ "{?CRA_Category <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_Rank}	"
//			+ "{?CRA_Rank <http://semoss.org/ontologies/Relation/Contains/Rank_Value> ?Rank}	"
//			+ "{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Prefix> ?Prefix}	"
//			+ "{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Suffix> ?Suffix}	"
//			+ "{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Value> ?Value}		"
//			+ "{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Is_Blank> 'Y'}"
//			+ "BIND(<@System@> AS ?System)}";
	
	private static String blankCategoryQuery = "SELECT DISTINCT ?System ?CRA_Archtype ?CRA_Category ?Prefix ?Value ?Suffix ?CRA_LOE_Rating ((?Rank)*(?LOE) AS ?Score) WHERE{{?System a <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/HasCRA> ?CRA_Value}{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}{?CRA_Archtype a <http://semoss.org/ontologies/Concept/CRA_Archtype>}{?CRA_Archtype <http://semoss.org/ontologies/Relation/Contains> ?CRA_Category}	{?CRA_Value a <http://semoss.org/ontologies/Concept/CRA_Value>}{?CRA_LOE_Rating a <http://semoss.org/ontologies/Concept/CRA_LOE_Rating>}{?CRA_Value <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_LOE_Rating}{?CRA_LOE_Rating <http://semoss.org/ontologies/Relation/Contains/LOE_Value> ?LOE}{?CRA_Category a <http://semoss.org/ontologies/Concept/CRA_Category>}{?CRA_Category <http://semoss.org/ontologies/Relation/Contain> ?CRA_Value}{?CRA_Rank a <http://semoss.org/ontologies/Concept/CRA_Rank>}{?CRA_Category <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_Rank}{?CRA_Rank <http://semoss.org/ontologies/Relation/Contains/Rank_Value> ?Rank}{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Prefix> ?Prefix}{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Suffix> ?Suffix}{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Value> ?Value}	{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Is_Blank> 'Y'}BIND(<@System@> AS ?System)}";

	//Query for getting information about archtypes for systems
//	private static String archTypeQuery = "SELECT DISTINCT ?System ?CRA_Archtype ?CRA_Category ?Prefix ?Value ?Suffix ?CRA_Assessment ((?Rank)*(?Assessment) AS ?Score) WHERE"
//					+"{"
//						+"{?System a <http://semoss.org/ontologies/Concept/System>}"
//						+"{?System <http://semoss.org/ontologies/Relation/HasCRA> ?CRA_Value}"
//						+"{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}"
//						+"{?CRA_Archtype a <http://semoss.org/ontologies/Concept/CRA_Archtype>}"
//						+"{?CRA_Archtype <http://semoss.org/ontologies/Relation/Contains> ?CRA_Category}"
//						+"{?CRA_Value a <http://semoss.org/ontologies/Concept/CRA_Value>} "
//						+"{?CRA_Assessment a <http://semoss.org/ontologies/Concept/CRA_Assessment>}"
//						+"{?CRA_Value <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_Assessment}" 
//						+"{?CRA_Assessment <http://semoss.org/ontologies/Relation/Contains/Assessment_Value> ?Assessment}" 
//						+"{?CRA_Category a <http://semoss.org/ontologies/Concept/CRA_Category>}"
//						+"{?CRA_Category <http://semoss.org/ontologies/Relation/Contain> ?CRA_Value}"
//						+"{?CRA_Rank a <http://semoss.org/ontologies/Concept/CRA_Rank>}"
//						+"{?CRA_Category <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_Rank}"
//						+"{?CRA_Rank <http://semoss.org/ontologies/Relation/Contains/Rank_Value> ?Rank}"
//						+"{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Prefix> ?Prefix}"
//						+"{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Suffix> ?Suffix}"
//						+"{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Value> ?Value}"
//						+"MINUS{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Is_Blank> 'Y'}"
//					+"BIND(<@System@> AS ?System)}";
	
	private static String archTypeQuery = "SELECT DISTINCT ?System ?CRA_Archtype ?CRA_Category ?Prefix ?Value ?Suffix ?CRA_LOE_Rating ((?Rank)*(?LOE) AS ?Score) WHERE{{?System a <http://semoss.org/ontologies/Concept/System>}{?System <http://semoss.org/ontologies/Relation/HasCRA> ?CRA_Value}{?System <http://semoss.org/ontologies/Relation/Contains/NeedsAssessment> 'Yes'}{?CRA_Archtype a <http://semoss.org/ontologies/Concept/CRA_Archtype>}{?CRA_Archtype <http://semoss.org/ontologies/Relation/Contains> ?CRA_Category}{?CRA_Value a <http://semoss.org/ontologies/Concept/CRA_Value>}{?CRA_LOE_Rating a <http://semoss.org/ontologies/Concept/CRA_LOE_Rating>}{?CRA_Value <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_LOE_Rating}{?CRA_LOE_Rating <http://semoss.org/ontologies/Relation/Contains/LOE_Value> ?LOE}{?CRA_Category a <http://semoss.org/ontologies/Concept/CRA_Category>}{?CRA_Category <http://semoss.org/ontologies/Relation/Contain> ?CRA_Value}{?CRA_Rank a <http://semoss.org/ontologies/Concept/CRA_Rank>}{?CRA_Category <http://semoss.org/ontologies/Relation/AssessedAs> ?CRA_Rank}{?CRA_Rank <http://semoss.org/ontologies/Relation/Contains/Rank_Value> ?Rank}{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Prefix> ?Prefix}{?CRA_Category <http://semoss.org/ontologies/Relation/Contains/Suffix> ?Suffix}{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Value> ?Value}MINUS{?CRA_Value <http://semoss.org/ontologies/Relation/Contains/Is_Blank> 'Y'}BIND(<@System@> AS ?System)}";
	
	//Data to be returned will be stored here
	Hashtable retHash;
	
	@Override
	public void createData() {
		String systemURI = query;
		String thisSystemDetailQuery = systemDetailQuery.replace("<@System@>", systemURI);
		
		//the location of the query data
		Hashtable newDataHash = new Hashtable();
		
		//Process system detail query
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(this.engine, thisSystemDetailQuery);
		String[] names = wrapper.getVariables();
		if(wrapper.hasNext()) { 
			ISelectStatement ss = wrapper.next();
			String system = ss.getRawVar(names[0]) + "";
			String systemName = ss.getVar(names[1]) + ""; //getVar for properties
			String description = ss.getVar(names[2]) + "";
			String[] systemInformation = new String[names.length - 4];
			
			//system information will be the column headers
			int x = 0;
			for(int i = 3; i < names.length - 1; i++) {
				systemInformation[x] = names[i] + " - " + ss.getRawVar(names[i]);
				x++;
			}
			
			String overallAssessment = ss.getRawVar(names[names.length-1])+ "";
			
			//put the information in the new data hash
			newDataHash.put("systemName", systemName);
			newDataHash.put("systemDescription", description);
			newDataHash.put("systemInformation", systemInformation);
			newDataHash.put("overallReadinessAssessment", overallAssessment);
		}
		
		
		//process blank category query
		List<HashMap<String, String>> unknownItems = new ArrayList<HashMap<String, String>>();
		String thisBlankCategoryQuery = blankCategoryQuery.replace("<@System@>", systemURI);
		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper(this.engine, thisBlankCategoryQuery);
		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext()) {
			HashMap<String, String> newUnknownItem = new HashMap<String, String>();
			
			ISelectStatement ss = wrapper2.next();
			String unknownItem = ss.getRawVar(names2[1]) + "";
			
			String system = ss.getRawVar(names2[0]) + "";
			String craArchtype = ss.getRawVar(names2[1]) + "";
			String cracategory = ss.getRawVar(names2[2]) + "";
			String prefix = ss.getRawVar(names2[3]) + "";
			String value = ss.getRawVar(names2[4]) + "";
			String suffix = ss.getRawVar(names2[5]) + "";
			String craAssessment = ss.getRawVar(names2[6]) + "";
			String score = ss.getVar(names2[7]) + "";
			
			String details = prefix+value+suffix;
			
			newUnknownItem.put("risk", craAssessment);
			newUnknownItem.put("details", prefix+value+suffix);
			
			unknownItems.add(newUnknownItem);
		
		}
		
		newDataHash.put("unknownItems", unknownItems);
		

		//process archtype query
		List<HashMap<String, Object>> archtypes = new ArrayList<HashMap<String, Object>>();
		String thisArchTypeQuery = archTypeQuery.replace("<@System@>", systemURI);
		System.out.println(thisArchTypeQuery);
		ISelectWrapper wrapper3 = WrapperManager.getInstance().getSWrapper(this.engine, thisArchTypeQuery);
		String[] names3 = wrapper3.getVariables();
		HashMap<String, HashMap<String, Object>> archtypeSet = new HashMap<>();
		while(wrapper3.hasNext()) {

			ISelectStatement ss = wrapper3.next();
			
			//Get all variables for the row
			String system = ss.getRawVar(names3[0]) + "";
			String craArchtype = ss.getRawVar(names3[1]) + "";
			String cracategory = ss.getRawVar(names3[2]) + "";
			String prefix = ss.getRawVar(names3[3]) + "";
			String value = ss.getRawVar(names3[4]) + "";
			String suffix = ss.getRawVar(names3[5]) + "";
			String craAssessment = ss.getRawVar(names3[6]) + "";
			String score = ss.getVar(names3[7]) + "";
			
			String details = prefix+value+suffix;
			
			ArrayList<HashMap<String, String>> riskStatus;
			HashMap<String, Object> nextArchType;
			
			if(archtypeSet.containsKey(craArchtype)) {

				nextArchType = archtypeSet.get(craArchtype);
				riskStatus = (ArrayList<HashMap<String, String>>) nextArchType.get("riskStatus");
				
			} else {
				
				nextArchType = new HashMap<String, Object>();
				nextArchType.put("name", craArchtype);
				riskStatus = new ArrayList<>();	
			
			}

			HashMap<String, String> riskStatusMap = new HashMap<>();
			riskStatusMap.put("risk", craAssessment);
			riskStatusMap.put("details", details);
			
			riskStatus.add(riskStatusMap);
			nextArchType.put("riskStatus", riskStatus);
			
			archtypeSet.put(craArchtype, nextArchType);
		}
		
		archtypeSet = sortRiskStatus(archtypeSet);
		for(String archtype : archtypeSet.keySet()) {
			archtypes.add(archtypeSet.get(archtype));
		}
		
		newDataHash.put("archtypes", archtypes);
		newDataHash.put("layout", "prerna.ui.components.specific.navypeo.NavyScoreboardPlaysheet");
		this.retHash = newDataHash;
	}
	
	
	@Override
	public Hashtable getDataMakerOutput(String... selectors) {		
		return this.retHash;
	}
	
	//sort the riskstatus objects in archtypes by its risk value
	//Complicated, moderate, Simple in that order
	private HashMap<String, HashMap<String, Object>> sortRiskStatus(HashMap<String, HashMap<String, Object>> archtypeSet) {
		
		Comparator comparator = new Comparator<Object>() {
			public int compare(Object o1, Object o2) {
				
				HashMap<String, String> map1 = (HashMap<String, String>)o1;
				HashMap<String, String> map2 = (HashMap<String, String>)o2;
				
				String m1 = map1.get("risk");
				String m2 = map2.get("risk");
				
				return m1.compareToIgnoreCase(m2);
			}
		};
		
		for(String key : archtypeSet.keySet()) {
			HashMap<String, Object> map = archtypeSet.get(key);
			List<Object> riskStatus = (ArrayList<Object>) map.get("riskStatus");
			Collections.sort(riskStatus, comparator);
		}
		
		return archtypeSet;
	}
}
