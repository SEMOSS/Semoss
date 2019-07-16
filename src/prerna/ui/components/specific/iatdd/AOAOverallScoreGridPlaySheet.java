package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ibm.icu.text.DecimalFormat;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.GridPlaySheet;

public class AOAOverallScoreGridPlaySheet extends GridPlaySheet {

	private AOAOverallScoreMap scoreMap = new AOAOverallScoreMap();
	private Map<String,AOAOverallScoreMap> scoreMapFE = new HashMap<String,AOAOverallScoreMap>();
	private Map<String, String> checkedPackages = new HashMap<String, String>();
	private Map<String, String> rankings = new HashMap<String, String>();
	private Map<String, String> ReqtoMissionTask = new HashMap<String, String>();
	private Map<String, String> ServiceScoreArmy = new HashMap<String, String>();
	private Map<String, String> ServiceScoreNavy = new HashMap<String, String>();
	private Map<String, String> ServiceScoreAirForce =new HashMap<String, String>();
	private ArrayList<Object> data = new ArrayList<Object>();

	public static final String FULFILLMENT_SCORE = "fScore";
	public static final String AND_PACKAGE_ARRAY = "ANDPackage";
	public static final String OR_PACKAGE_ARRAY ="ORPackage";
	public static final String ROW_AND_ARRAY = "rAnd";

	private boolean useArmy = true;
	private boolean useNavy = true;
	private boolean useAirForce = true;

	private static final String RANKINGS_QUERY = "SELECT DISTINCT ?Mission_Task ?Ranking_Score WHERE{ {?Mission_Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Mission_Task>;} {?Mission_Task <http://semoss.org/ontologies/Relation/Contains/Ranking_Score> ?Ranking_Score;} }"; 

	private static final String MISSION_TASK_REQ = "SELECT DISTINCT ?Sub_Level_Requirement ?Mission_Task"
			+" WHERE{"
			+"{?Vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;}"
			+"{?Vendor ?supports ?Package;}" 
			+"{?Package_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Package_Requirement>;}" 
			+"{?fufills <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Fulfills> ;}" 
			+"{?Package ?fufills ?Package_Requirement;}" 
			+"{?Sub_Level_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Requirement>;}"
			+"{?fufills2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Fulfills> ;}" 
			+"{?Package_Requirement ?fufills2 ?Sub_Level_Requirement;}" 
			+"{?Functional_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Functional_Requirement>;}" 
			+"{?supports2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}" 
			+"{?Sub_Level_Requirement ?supports2 ?Functional_Requirement;}" 
			+"{?Mission_Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Mission_Task>;}" 
			+"{?supports3 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}" 
			+"{?Functional_Requirement ?supports3 ?Mission_Task;}" 
			+"{?Mission_Task <http://semoss.org/ontologies/Relation/Contains/Ranking_Score> ?Ranking_Score;}"
			+"}"; 

	private static final String SERVICE_SCORE_QUERY = "SELECT DISTINCT ?Sub_Level_Requirement ?Air_Force_Service_Score ?Army_Service_Score ?Navy_Service_Score" 
			+" WHERE{"
			+"{?Sub_Level_Requirement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Requirement>;}"
			+"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Contains/Air_Force_Service_Score> ?Air_Force_Service_Score;}" 
			+"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Contains/Navy_Service_Score> ?Navy_Service_Score;}" 
			+"{?Sub_Level_Requirement <http://semoss.org/ontologies/Relation/Contains/Army_Service_Score> ?Army_Service_Score;}" 
			+"}";

	public void createData() {
		//create score map from query
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String vendor = (String) ss.getVar(names[0]);
			String packages = (String) ss.getVar(names[1]);
			String requirement = (String) ss.getVar(names[2]);
			double fScore = (double) ss.getVar(names[5]);
			boolean rAnd = (boolean) Boolean.parseBoolean((String) ss.getVar(names[6]));
			String ANDPackage= (String) ss.getVar(names[7]);
			//			if (ANDPackage.isEmpty() || ANDPackage == null){
			//				ANDPackage = "";
			//			};
			String ORPackage = (String) ss.getVar(names[8]);
			//			if (ORPackage.isEmpty() || ORPackage == null){
			//				ORPackage = "";
			//			};

			scoreMap.compileArrays(vendor, requirement, packages, fScore, ANDPackage, ORPackage, rAnd);
		}
	}

	public void runAnalytics() {
		if (useArmy == false && useNavy == false && useAirForce == false) {
			throw new IllegalArgumentException("PICK A STAKEHOLDER");
		}
		//format to send to Front-End
		DecimalFormat formatter = new DecimalFormat("#.##");
		Map<String, Set<String>> vendorReqHash = scoreMap.getVendorReqHash();
		String[] newHeaders = new String[]{"Vendor","Score"};
		dataFrame = new H2Frame(newHeaders);
		for(String vendor : vendorReqHash.keySet()){
			Object[] row = new Object[]{vendor, formatter.format(overallVendorScore(vendor, vendorReqHash.get(vendor)))};
			dataFrame.addRow(row, newHeaders);
			Hashtable<String, Object> rowValues = new Hashtable<String, Object>();
			rowValues.put("Vendor", vendor);
			rowValues.put("Score", formatter.format(overallVendorScore(vendor, vendorReqHash.get(vendor))));
			data.add(rowValues);
		}
	}


	@Override
	public void processQueryData() {
		super.processQueryData();
	}

	//set CheckedPackages from user input 
	public void setCheckedPackages(Map<String, String> checkedpackages) {
		this.checkedPackages = checkedpackages;
		System.out.println("CHECKED PACKAGES" + checkedpackages);
	}
	
	//TODO -- DISCUSSION ON UPDATING LOGIC POTENTIALLY
	public double calculateRowAndLogic(List<Map<String, Object>> orderedResults){
		double numDependenciesIncluded = 0.00;
		double totalDependencies = orderedResults.size();
		double maxVendorScore = 4.00;
		double overallVendorScoreAddition = 0.00; //perReq
		
		ORDERED_AND_ROW_VALUES: for(int i = 0; i < orderedResults.size(); i++) {
			Map<String,Object> currentpackage = orderedResults.get(i);
			String packageName = (String) currentpackage.get("packageName");
			
			if( (checkedPackages.containsKey(packageName) && checkedPackages.get(packageName).equals("true") ) || (checkedPackages.isEmpty())) {			
				ArrayList<String> dependentAndPackages = (ArrayList<String>) currentpackage.get(AND_PACKAGE_ARRAY);
				ArrayList<String> dependentOrPackages = (ArrayList<String>) currentpackage.get(OR_PACKAGE_ARRAY);

				boolean metRequirements = false;
				if (dependentAndPackages.size() != 0) {
					for (String depAndPackage: dependentAndPackages){
						if (checkedPackages.containsKey(depAndPackage) && checkedPackages.get(depAndPackage).equals("true") || (checkedPackages.isEmpty())){
							metRequirements = true;
						}
						else {
							metRequirements = false;
							continue ORDERED_AND_ROW_VALUES; 
						}
					}
				}
				else if (dependentOrPackages.size() != 0 && !metRequirements) {
					for (String depOrPackage: dependentOrPackages){
						if (checkedPackages.containsKey(depOrPackage) && checkedPackages.get(depOrPackage).equals("true") || (checkedPackages.isEmpty())){
							metRequirements = true;
							break;
						}
					}
				}
				else { 
					metRequirements= true;
				}

				if (metRequirements) {
					numDependenciesIncluded++;
				}
			}
		}
		
		overallVendorScoreAddition = maxVendorScore * (numDependenciesIncluded/totalDependencies);
		
		return overallVendorScoreAddition;
	}

	public double addtoOverallVendorScore(List<Map<String, Object>> orderedResults) { 
		double overallVendorScoreAddition = 0; //perReq
		
		if( (boolean) orderedResults.get(0).get(ROW_AND_ARRAY) ) {
 			overallVendorScoreAddition = calculateRowAndLogic(orderedResults); 
			return overallVendorScoreAddition;
		}
		
		ORDERED_VALUES : for(int i = 0; i < orderedResults.size(); i++) {
			Map<String,Object> currentpackage = orderedResults.get(i);
			String packageName = (String) currentpackage.get("packageName");
			if( (checkedPackages.containsKey(packageName) && checkedPackages.get(packageName).equals("true") ) || (checkedPackages.isEmpty())) {			
					ArrayList<String> dependentAndPackages = (ArrayList<String>) currentpackage.get(AND_PACKAGE_ARRAY);
					ArrayList<String> dependentOrPackages = (ArrayList<String>) currentpackage.get(OR_PACKAGE_ARRAY);

					boolean metRequirements = false;
					if (dependentAndPackages.size() != 0) {
						for (String depAndPackage: dependentAndPackages){
							if (checkedPackages.containsKey(depAndPackage) && checkedPackages.get(depAndPackage).equals("true") || (checkedPackages.isEmpty())){
								metRequirements = true;
							}
							else {
								metRequirements = false;
								continue ORDERED_VALUES; 
							}
						}
					}
					else if (dependentOrPackages.size() != 0 && !metRequirements) {
						for (String depOrPackage: dependentOrPackages){
							if (checkedPackages.containsKey(depOrPackage) && checkedPackages.get(depOrPackage).equals("true") || (checkedPackages.isEmpty())){
								metRequirements = true;
								break;
							}
						}
					}
					else { 
						metRequirements= true;
					}

					if (metRequirements) {
						overallVendorScoreAddition = (double) currentpackage.get(FULFILLMENT_SCORE);
						return overallVendorScoreAddition;
					}
			}
		}
		return overallVendorScoreAddition;
	}

	public double overallVendorScore(String vendor, Set<String> requirement){
		double overallvendorscore = 0;
		int denominator = 0;
		for(String req : requirement) {
			try
			{
				//System.out.println(req);
				List<Map<String, Object>> orderedResults = scoreMap.orderFulfillmentScoresForVendorAndRequirement(vendor, req);
				//get fulfillment score
				double vscore = addtoOverallVendorScore(orderedResults);

				//get service score
				int count = 0;
				double sscore= 0;
				if(useArmy == true){
					sscore += Double.parseDouble(ServiceScoreArmy.get(req));
					count ++;
				}
				if(useAirForce == true){
					sscore += Double.parseDouble(ServiceScoreAirForce.get(req));
					count ++;
				}

				if(useNavy == true){
					sscore += Double.parseDouble(ServiceScoreNavy.get(req));
					count ++;
				}

				sscore /= count;

				//get ranking score
				String mission = ReqtoMissionTask.get(req);
				String rankingStr = rankings.get(mission);
				double rscore = Double.parseDouble(rankingStr);

				//to escalate PM and MM to P and M so cost modification isn't penalizing product 
				if(vscore == 1 || vscore == 3)
					vscore = vscore + 1;

				//calculate overall vendor score
				overallvendorscore += (vscore * rscore * sscore);
				denominator += (4 * rscore * sscore);

				System.out.println(vendor + ">>>" + req + ">>>>" + vscore + ">>>" + rscore + ">>>" + sscore);
			}
			catch(Exception e){
			}
		}
		overallvendorscore /= denominator ;	

		//truncate trailing decimals caused by division 
		if(overallvendorscore > 1){
			DecimalFormat formatter = new DecimalFormat("#.##");		
			overallvendorscore = Double.parseDouble(formatter.format(overallvendorscore));
		}

		int hundred = 100;
		overallvendorscore *= hundred;

		return overallvendorscore;
	}
	
	//set RankingScore from user input
	public void setRankings(Map<String, String> rankings) {
		this.rankings = rankings;
	}

	// run default RankingScore
	public void runDefaultRankingScore() {
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, RANKINGS_QUERY);
		String[] names = wrapper.getVariables();
		System.out.println(Arrays.toString(names));
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			System.out.println(ss.getVar(names[0]) + " " + ss.getVar(names[1]));
			rankings.put(ss.getVar(names[0]).toString(), (ss.getVar(names[1]).toString())); 
		}
	}

	public void runReqtoMissionTask() {
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, MISSION_TASK_REQ );
		String[] names = wrapper.getVariables();
		System.out.println(Arrays.toString(names));
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			System.out.println(ss.getVar(names[0]) + " " + ss.getVar(names[1]));
			ReqtoMissionTask.put(ss.getVar(names[0]).toString(), (ss.getVar(names[1]).toString())); 
		}
	}

	public void setUseArmy (boolean useArmy){
		this.useArmy = useArmy;
	}
	public void setUseNavy (boolean useNavy){
		this.useNavy = useNavy;
	}

	public void setUseAirForce (boolean useAirForce){
		this.useAirForce = useAirForce;
	}

	public void setStakeHolders(Map<String, Boolean> stakeholders) {
		for(String s : stakeholders.keySet()) {
			if(s.equalsIgnoreCase("ARMY")) {
				setUseArmy(stakeholders.get(s));
			} else if(s.equalsIgnoreCase("NAVY")) {
				setUseNavy(stakeholders.get(s));
			} else if(s.equalsIgnoreCase("AIRFORCE")) {
				setUseAirForce(stakeholders.get(s));
			} else {
				throw new IllegalArgumentException("Stakeholder " + s + " is not found...");
			}
		}
		data = new ArrayList<Object>();
	}

	public void runReqtoStakeHolderScore(){
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, SERVICE_SCORE_QUERY);
		String[] names = wrapper.getVariables();
		System.out.println(Arrays.toString(names));
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String req = ss.getVar(names[0]).toString();
			String airForceScore = ss.getVar(names[1]).toString();
			String armyScore = ss.getVar(names[2]).toString();
			String navyScore = ss.getVar(names[3]).toString();

			ServiceScoreAirForce.put(req, airForceScore);
			ServiceScoreArmy.put(req, armyScore); 
			ServiceScoreNavy.put(req, navyScore); 
		}
	}

	public ArrayList<Object> getData(){
		return this.data;

	}

}