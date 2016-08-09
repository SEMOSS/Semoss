package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.ds.H2.H2Frame;
import prerna.ds.H2.H2Joiner;
import prerna.om.Dashboard;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class DashboardJoinReactor extends AbstractReactor {

	
	/*
	 * TODO: THIS REACTOR IS NOT DONE YET... STILL IN INTIAL PHASE OF DEVELOPMENT...
	 */
	
	public DashboardJoinReactor() {
		// example join is data.join("insight1", "insight2", c:Title, c:Movie_Title, inner.join);
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM, PKQLEnum.COL_DEF, PKQLEnum.REL_TYPE, PKQLEnum.OPEN_DATA, PKQLEnum.OPEN_DATA+"insightid", "G"};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DASHBOARD_JOIN;
	}

	@Override
	public Iterator process() {

		System.out.println("entered dashboard join");
		System.out.println("mystore has...");
		for(String s : myStore.keySet()) {
			System.out.println(s + "\t\t" + myStore.get(s));
		}
		
		// insightsToJoin contains the two insights that are being joined together
		// colsForInsightsToJoin are the two columns that are being used within these two joins
		// order matters for these two lists!
		// the first entry in insightsToJoin corresponds with the first entry in colsForInsightsToJoin
		
		List<String> insightsToJoin;
		List<String> colsForInsightsToJoin;
		try {
			insightsToJoin = (List<String>) myStore.get(PKQLEnum.WORD_OR_NUM);
			if(insightsToJoin == null) {
				insightsToJoin = (List<String>) myStore.get(PKQLEnum.OPEN_DATA+"insightid"); 
			}
			colsForInsightsToJoin = (List<String>) myStore.get(PKQLEnum.COL_DEF);
		} catch(Exception e) {
			System.err.println("Error retrieving insights");
			return null;
		}
		String joinType = (String) myStore.get(PKQLEnum.REL_TYPE);
		
		Insight in1 = InsightStore.getInstance().get(insightsToJoin.get(0));
		Insight in2 = InsightStore.getInstance().get(insightsToJoin.get(1));
		
		Dashboard dashboard = (Dashboard)myStore.get("G");
		dashboard.addInsight(in1);
		dashboard.addInsight(in2);
		
		
		if(in1 == null || in2 == null) {
			System.err.println("insights not found...");
			return null;
		}
		
		H2Frame frame1 = null;
		String[] frame1Headers = null;
		H2Frame frame2 = null;
		String[] frame2Headers = null;

		try {
			frame1 = (H2Frame) in1.getDataMaker();
			frame1Headers = frame1.getColumnHeaders();
			
			frame2 = (H2Frame) in2.getDataMaker();
			frame2Headers = frame2.getColumnHeaders();
			
			Map<String, String> joinCols = new HashMap<String, String>(2);
			joinCols.put(colsForInsightsToJoin.get(0), colsForInsightsToJoin.get(1));
			
			H2Joiner.joinFrames(frame1, frame2, joinCols);
		} catch (ClassCastException e) {
			System.err.println("currently can only join h2 frames...");
			return null;			
		} catch (Exception e) {
			System.err.println("Join error");
			return null;
		}
		
		
		// update the data id so FE knows data has been changed
		frame1.updateDataId();
		frame2.updateDataId();

		
//		
//		String table1 = frame1.getTableNameForUniqueColumn(frame1Headers[frame1Headers.length-1]);
//		String table2 = frame2.getTableNameForUniqueColumn(frame2Headers[frame2Headers.length-1]);
//
//		String viewQuery = "CREATE VIEW TEST_VIEW AS (SELECT * FROM " + table1;
//		if(joinType.equals("inner.join")) {
//			viewQuery += " INNER JOIN " + table2;
//		} else if(joinType.equals("left.join")) {
//			//TODO expand list
//		}
//		
//		
//		
//		viewQuery += " ON " + table1 + "." + colsForInsightsToJoin.get(0) + " = " + table2 + "." + colsForInsightsToJoin.get(1) + ");";
//		
//		System.out.println("view query is: ");
//		System.out.println(viewQuery);
		
		return null;
	}

}
