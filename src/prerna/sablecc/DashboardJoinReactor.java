package prerna.sablecc;

import java.util.Iterator;
import java.util.List;

import prerna.ds.H2.H2Frame;
import prerna.om.Insight;
import prerna.om.InsightStore;

public class DashboardJoinReactor extends AbstractReactor {

	
	/*
	 * TODO: THIS REACTOR IS NOT DONE YET... STILL IN INTIAL PHASE OF DEVELOPMENT...
	 */
	
	public DashboardJoinReactor() {
		// example join is data.join("insight1", "insight2", c:Title, c:Movie_Title, inner.join);
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM, PKQLEnum.COL_DEF, PKQLEnum.REL_TYPE};
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
		List<String> insightsToJoin = (List<String>) myStore.get(PKQLEnum.WORD_OR_NUM);
		List<String> colsForInsightsToJoin = (List<String>) myStore.get(PKQLEnum.COL_DEF);
		
		String joinType = (String) myStore.get(PKQLEnum.REL_TYPE);
		
		Insight in1 = InsightStore.getInstance().get(insightsToJoin.get(0));
		Insight in2 = InsightStore.getInstance().get(insightsToJoin.get(1));
		
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
		} catch (ClassCastException e) {
			System.err.println("currently can only join h2 frames...");
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
