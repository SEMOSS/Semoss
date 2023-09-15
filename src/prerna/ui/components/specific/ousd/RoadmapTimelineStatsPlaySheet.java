package prerna.ui.components.specific.ousd;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.annotations.BREAKOUT;

@BREAKOUT
public class RoadmapTimelineStatsPlaySheet extends RoadmapCleanTableComparisonPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(RoadmapTimelineStatsPlaySheet.class.getName());
	String roadmap;
	String owner;
	
	// NAMING
	protected final String decomCount = "System Decommission Count";
	protected final String savingThisYear = "New Savings this year";
	protected final String buildCount = "New Interface Count";
	protected final String investmentCost = "Interface Development Cost";
	protected final String sustainCost = "Interface Sustainment Cost";
	protected final String risk = "Enterprise Risk";
	
	protected final String cumSavings = "Cumulative Savings";
	protected final String prevSavings = "Previous Decommissioning Savings";
	protected final String cumCost = "Cumulative Cost";
	protected final String roi = "ROI";
	protected final String opCost = "Operational Cost";

	@Override
	public void setQuery(String query){
		String delimiters = "[;]";
		String[] insights = query.split(delimiters);
		roadmap = insights[0];
		timelineNames.add(roadmap);
		if(insights.length>1){
			owner = insights[1];
		}

	}

	@Override
	public void createData(){
		List<String> roadmapName = new ArrayList<String>();
		roadmapName.add(roadmap); 
		
		buildTable(roadmapName, owner);
	}

}
