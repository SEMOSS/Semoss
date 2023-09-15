package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.annotations.BREAKOUT;

@BREAKOUT
public class ActivityGroupRiskCalculator {

	double placeholderFailureChance = 0.05;
	List<String>  sysList = new ArrayList<String>();

	Map<String, Map<String, List<String>>> activityToBluToSystem = new HashMap<String, Map<String, List<String>>>();
	Map<String, Double> systemFailureMap = new HashMap<String, Double>();

	Map<String, List<String>> groupToActivities;
	Map<String, List<String>> groupToDependencies;
	Map<String, List<String>> criticalSystems = new HashMap<String, List<String>>();
	List<String> orderedGroups;


	public void setGroupData(Map<String, List<String>> groupToActivities, Map<String, List<String>> groupToDependencies, List<String> orderedGroups){
		this.groupToActivities = groupToActivities;
		this.groupToDependencies = groupToDependencies;
		this.orderedGroups = orderedGroups;
	}
	
	public void setFailure(Double fail){
		this.placeholderFailureChance = fail;
	}

	public void setBluMap(Map<String, Map<String, List<String>>> bluMap){
		this.activityToBluToSystem = bluMap;
	}

	public void setData(List<String> sysList){
		this.sysList = sysList;
	}

	public Map<String, Double> runRiskCalculations(){
		Map<String, Double> activityGroupRisks = determineHierarchyTreeRisk();
		return activityGroupRisks;
	}

	//determines the risk for the entire tree
	private Map<String, Double> determineHierarchyTreeRisk(){
		Map<String, Double> treeValues = new HashMap<String, Double>();
		for(String group: orderedGroups){
			List<String> activities = groupToActivities.get(group);
			double groupRisk = determineActivityGroupRisk(activities);
			treeValues.put(group, groupRisk);
			for(String parent: groupToDependencies.keySet()){
				List<String> children = groupToDependencies.get(parent);
				if(children != null && children.contains(group)){
					groupRisk = (double)1-((double)1-treeValues.get(parent))*((double)1-groupRisk);
				}
			}
			treeValues.put(group, groupRisk);
		}
		return treeValues;
	}

	//determines the risk for a given activity group
	private Double determineActivityGroupRisk(List<String> activities){		
		Map<String, Double> activityRisks = determineActivityRisk(activities); 

		double groupRisk = 1.0;
		double determinedActivityRisk = 1.0;

		for(String activity: activityRisks.keySet()){
			double value = (double)1.0 - (double) activityRisks.get(activity);
			determinedActivityRisk = determinedActivityRisk * value;
		}
		groupRisk = groupRisk - determinedActivityRisk;
		return groupRisk;
	}

	//determines the risk for an activity
	private Map<String, Double> determineActivityRisk(List<String> activitiesInActivityGroup){
		Map<String, Double> activityGroupRisk = new HashMap<String, Double>();

		for(String activity: activitiesInActivityGroup){
			double determinedBluRisk = 1.0;
			double activityRisk = 1.0;
			Map<String, Double> bluRisk = determineBLURisk(activityToBluToSystem.get(activity));
			for(String blu: bluRisk.keySet()){
				double value = 1.0 - bluRisk.get(blu);
				determinedBluRisk = determinedBluRisk * value;
			}
			activityRisk = activityRisk - determinedBluRisk;
			activityGroupRisk.put(activity, activityRisk);
		}

		return activityGroupRisk;
	}

	//determines the risk for a BLU
	private Map<String, Double> determineBLURisk(Map<String, List<String>> bluSystem){
		Map<String, Double> bluRiskMap = new HashMap<String, Double>();

		if(bluSystem == null){ // if no systems supporting the blu or no blu needed for the activity, there is no risk associated with this activity
			return bluRiskMap;
		}

		for(String blu: bluSystem.keySet()){
			double bluRisk = 1.0;
			List<String> supportingSystems = bluSystem.get(blu);
			for(String suppSys: supportingSystems){
				//placeholder logic for the probability of failure for a system
				//bluRisk = bluRisk * systemFailureMap.get(suppSys);
				if(supportingSystems.size() == 1 && !sysList.contains(suppSys)){
//					System.out.println("WE HAVE UNIQUE DATA/BLU ::::::: " + blu);
//					System.out.println("The system supporting it " + suppSys + " has been killed");
					List<String> missingBLU = criticalSystems.get(suppSys);
					if(missingBLU == null){
						missingBLU = new ArrayList<String>();
					}
					if(!missingBLU.contains(blu)){
						missingBLU.add(blu);
					}
					criticalSystems.put(suppSys, missingBLU);
					continue;
				}else if (sysList.contains(suppSys)){
					bluRisk = bluRisk * placeholderFailureChance;
					bluRiskMap.put(blu, bluRisk);
				}
			}
		}

		return bluRiskMap;
	}
	
	public Map<String, List<String>> getCriticalSystems(){
		return this.criticalSystems;
	}

}
