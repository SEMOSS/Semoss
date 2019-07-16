package prerna.ui.components.specific.iatdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.HeatMapPlaySheet;
import prerna.util.Utility;

public class AOAHeatMapPlaySheet extends HeatMapPlaySheet{

	private Map<String, String> rankings = new HashMap<String, String>();
	private Map<String, Map<String, Double>> packageMissionHash = new HashMap<String, Map<String, Double>>();
	private Map<String, Map<String,Map<String,Double>>> packageMissionHashFE = new HashMap<String, Map<String, Map<String,Double>>>();
	private ITableDataFrame origDataFrame;
	private ArrayList<Object> data = new ArrayList<Object>();
	
	private static final String RANKINGS_QUERY = "SELECT DISTINCT ?Mission_Task ?Ranking_Score WHERE{ {?Mission_Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Mission_Task>;} {?Mission_Task <http://semoss.org/ontologies/Relation/Contains/Ranking_Score> ?Ranking_Score;} }"; 
	
	private boolean useArmy = true;
	private boolean useNavy = true;
	private boolean useAirForce = true;
	
	@Override
	public void runAnalytics() {
		if(origDataFrame == null) {
			origDataFrame = dataFrame;
		}
		
		String[] headers = origDataFrame.getColumnHeaders();
		Iterator<IHeadersDataRow> it = origDataFrame.iterator();	
		while(it.hasNext()) {
			Object[] row = it.next().getValues();
			System.out.println(row);
			
			//calculate ServiceScore based on checked services 
			double avgSS = 0;
			int countNum = 0;
			if(useAirForce) {
				avgSS += ((double) row[0]);
				countNum++;
			}
			if(useArmy) {
				avgSS += ((double) row[1]);
				countNum++;
			}
			if(useNavy) {
				avgSS += ((double) row[4]);
				countNum++;
			}
			
			avgSS /= countNum;
			
			//declare RankingScore based on user given mission task rankings 
			double reqValue = Double.parseDouble(rankings.get(Utility.getInstanceName(row[3].toString())));
			
			//declare FulfillmentScore based on average of all fulfillment scores that product had for each mission task (done in query) 
			double fulfillment = ((double) row [2]);
			
			//calculate ProductScore based on product's ServiceScore, RankingScore and FulfillmentScore
			double avgPS = (avgSS * fulfillment * reqValue);	
			
			String mission = (String) row[3];
			String packages = (String) row[5];
			
			// add package to mission to package score
			if(packageMissionHash.containsKey(packages)) {
				Map<String, Double> innerMap = packageMissionHash.get(packages);
				innerMap.put(mission, avgPS);
			} else {
				Map<String, Double> innerMap = new HashMap<String, Double>();
				innerMap.put(mission, avgPS);
				packageMissionHash.put(packages, innerMap);
			}
		}
		
		//re-order packages and format to send to front end
		String[] newHeaders = new String[]{headers[5], headers[3], "Package_Score"};
		dataFrame = new H2Frame(newHeaders);
		for(String packages : packageMissionHash.keySet()) {
			Map<String, Double> missionHash = packageMissionHash.get(packages);
			for(String mission : missionHash.keySet()) {
				Double packageScore = missionHash.get(mission);
				Object[] row = new Object[]{packages, mission, packageScore};
				System.out.println("ORDERING: "+ packages +" "+ mission +" "+ packageScore);
				dataFrame.addRow(row, newHeaders);
				Hashtable<String, Object> rowValues = new Hashtable<String, Object>();
				rowValues.put(headers[5], packages);
				rowValues.put(headers[3], mission);
				rowValues.put("Package Score", packageScore);
				data.add(rowValues);
			}
		}
	}
	
	
	@Override
	public void processQueryData() {
		//not utilizing inherited function
		super.processQueryData();
		
	}
	
	//run default RankingScore for the first time, user has yet to assigned rankings 
	public void runDefaultRankingScore() {
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, RANKINGS_QUERY);
		String[] names = wrapper.getVariables();
		System.out.println(Arrays.toString(names));
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			//TODO: should have this store requirements as URIs
			rankings.put(ss.getVar(names[0]).toString(), (ss.getVar(names[1]).toString())); 
		}
	}
	
	//set Rankings according to user input 
	public void setRankings(Map<String, String> rankings) {
		this.rankings = rankings;
	}
	
	//set Services according to user input 
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
	
	public void setUseArmy (boolean useArmy){
		this.useArmy = useArmy;
	}
	public void setUseNavy (boolean useNavy){
		this.useNavy = useNavy;
	}
	
	public void setUseAirForce (boolean useAirForce){
		this.useAirForce = useAirForce;
	}
	
	public ArrayList<Object> getData() {
		return data;
	}
	
}
