package prerna.ds.H2;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.ds.H2.H2Builder.Comparator;
import prerna.om.Dashboard;
import prerna.om.Insight;

/**
 * 
 * 
 *
 */
public class H2Joiner {

	private static int counter = 0;
	private static String tableNamePrefix = "VIEWTABLE94328"; //something random...hopefully no one else makes a table named this
	
	//this will map the name of a view table to List of H2 frames
	private Hashtable<String, List<H2Frame>> joinedFrames = new Hashtable<>();
	
	//mapping from 2 frames to joinColumns
	//view table -> {[frame, frame] -> [joinCol, joinCol]}
	//used this for original select query
	private Hashtable<String, Map<Object, Object>> joinColsMap = new Hashtable<>();
	private Dashboard dashboard;
	
	public H2Joiner() {
		
	}
	
	public H2Joiner(Dashboard dashboard) {
		this.dashboard = dashboard;
	}

	/**
	 * 
	 * @param frame1
	 * @param frame2
	 * @param joinCols - columns to be joined on, keys in map refer to columns in frame1, values refer to columns in frame2
	 */
	public void joinFrames(H2Frame frame1, H2Frame frame2, Map<String, String> joinCols) {
		//make sure both frames have the same schema
		
		//if both frames are already joined, throw error
		boolean frame1joined = frame1.isJoined();
		boolean frame2joined = frame2.isJoined();
		
		if(frame1joined && frame2joined) {
			throw new IllegalArgumentException("Both Frames are already joined");
		} else if(frame1joined) {
			//join frame2 to frame1
			String viewTable1 = frame1.builder.getViewTableName().toUpperCase();
			H2Frame[] frameArrayKey = {frame1, frame2};
			
			try {
				//get table name from frame1 and build query
				Map map = joinColsMap.get(viewTable1);
				map.put(frameArrayKey, joinCols);
				
				List<H2Frame> h2List = joinedFrames.get(viewTable1);
				h2List.add(frame2);
				
				String buildViewQuery = buildView(viewTable1);
				frame1.builder.runExternalQuery(buildViewQuery);
				
				frame2.setJoin(viewTable1);
			} catch (Exception e) {
				e.printStackTrace();
				joinedFrames.get(viewTable1).remove(frame2);
				joinColsMap.get(viewTable1).remove(frameArrayKey);
			}
		} else if(frame2joined) {
			//join frame 1 to frame 2
			String viewTable2 = frame2.builder.getViewTableName().toUpperCase();
			H2Frame[] frameArrayKey = {frame1, frame2};
			try {
				
				Map map = joinColsMap.get(viewTable2);
				map.put(frameArrayKey, joinCols);
				
				List<H2Frame> h2List = joinedFrames.get(viewTable2);
				h2List.add(frame1);
				
				String buildViewQuery = buildView(viewTable2);
				frame1.builder.runExternalQuery(buildViewQuery);
				
				frame1.setJoin(viewTable2);
				
			} catch (Exception e) {
				e.printStackTrace();
				joinedFrames.get(viewTable2).remove(frame1);
				joinColsMap.get(viewTable2).remove(frameArrayKey);
			}
			
		} else {
			//create new table, join to each other
			String newTable = getNewTableName();
			try {
				
				List<H2Frame> newFrameList = new ArrayList<>(3);
				newFrameList.add(frame1);
				newFrameList.add(frame2);
				joinedFrames.put(newTable, newFrameList);
				
				Map map = new HashMap();
				H2Frame[] joinedFrames = {frame1, frame2};
				map.put(joinedFrames, joinCols);
				joinColsMap.put(newTable, map);
				
				String buildViewQuery = buildView(newTable);
				frame1.builder.runExternalQuery(buildViewQuery);
				
				frame1.setJoin(newTable);
				frame2.setJoin(newTable);
				
			} catch (Exception e) {
				e.printStackTrace();
				joinedFrames.remove(newTable);
				joinColsMap.remove(newTable);
			}
		}
	}
	
	public void joinFrame(H2Frame frame1, H2Frame frame2, List<List<String>> joinCols) {
		
	}

	
//	/**
//	 * 
//	 * @param frames		This method unjoins a list of frames
//	 * 							if last frame is for a view is unjoined, the view table will also be dropped
//	 * 							if the second to last frame for a view, that frame will also be unjoined
//	 * 							
//	 * 							Any affected views with remaning frames will be refreshed to be reflective of the loss of data 
//	 * 			
//	 */
//	public void unJoinFrame(H2Frame... frames){
//		
//		//set of views to refresh
//		Set<String> viewsToRefresh = new HashSet<String>(frames.length);
//		
//		//for each frame if the frame is joined
//		for(H2Frame frame : frames) {
//			if(frame.isJoined()) {
//				
//				//remove the frame from the frame list
//				String viewTable = frame.builder.getViewTableName();
//				List<H2Frame> frameList = joinedFrames.get(viewTable);
//				frameList.remove(frame);
//				
//				//add the table to refresh set
//				viewsToRefresh.add(viewTable);
//				
//				//last frame can't be joined to anything so unjoin that as well
//				if(frameList.size() == 1) {
//					unJoinFrame(frameList.get(0));
//				} 
//				
//				//if no more frames left just drop the view
//				else if(frameList.isEmpty()) {
//					//drop the view table
//					frame.builder.dropView();
//					joinedFrames.remove(viewTable);
//				}
//				
//				//finally call unjoin on the frame letting the frame know it is unjoined
//				frame.unJoin();
//			}
//		}
//		
//		//for each view refresh if it is still associated with joined frames
//		for(String view : viewsToRefresh) {
//			List<H2Frame> frameList = joinedFrames.get(view);
//			if(frameList != null) {
//				H2Frame frame = frameList.get(0);
//				refreshView(frame, view);
//			}
//		}
//	}
	
	/**
	 * 
	 * @param frame
	 * @param viewTable
	 */
	public void refreshView(H2Frame frame, String viewTable) {
		try {
//			String buildViewQuery = buildView(viewTable);
//			frame.builder.runExternalQuery(buildViewQuery);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param tableName			name of the view to get filters for
	 * @return					a merge of all the filter data from the frames associated with the tableName (view) parameter
	 */
	protected Map<String, Map<Comparator, Set<Object>>> getJoinedFilterHash(String tableName) {
//		List<Insight> insights = dashboard.getInsights();
		
		List<H2Frame> frames = getJoinedFrames();
		
		//build the filter query from the frames
		Map<String, Map<Comparator, Set<Object>>> filterHash = new HashMap<>();
		for(H2Frame frame : frames) {
			Map<String, Map<Comparator, Set<Object>>> nextHash = frame.builder.filterHash2;
			for(String column : nextHash.keySet()) {
				String translatedColumn = frame.builder.joinColumnTranslation.get(column);
				if(filterHash.containsKey(translatedColumn)) {
					Map<Comparator, Set<Object>> columnHash = filterHash.get(column);
					Map<Comparator, Set<Object>> nextColumnHash = nextHash.get(column);
					for(Comparator comparator : nextColumnHash.keySet()) {
						if(columnHash.containsKey(comparator)) {
							columnHash.get(comparator).addAll(nextColumnHash.get(comparator));
						} else {
							columnHash.put(comparator, nextColumnHash.get(comparator));
						}
					}
				} else {
					filterHash.put(translatedColumn, nextHash.get(column));
				}
			}
		}
		
		return filterHash;
	}
	
	private List<H2Frame> getJoinedFrames() {
		List<Insight> insights = dashboard.getInsights();
		
		List<H2Frame> frames = new ArrayList<>();//joinedFrames.get(tableName);
		for(Insight insight : insights) {
			frames.add((H2Frame)insight.getDataMaker());
		}
		return frames;
	}
	/**
	 * 
	 * @param newTableName 			table name of the view that will be created
	 * @return						The sql query that will build the view
	 * 
	 * Example of return : "CREATE VIEW newTableName AS (SELECT Frame1.TITLE, Director, Genre FROM Frame1 INNER JOIN Frame2 ON Frame1.TITLE = Frame2.TITLE)"
	 * 
	 * Create view newtable as (Select frame1.title, director, genre, studio, from frame1 inner join frame2 on frame1.title = frame2.title 
	 * 
	 * TODO : need to change this to be outer, i.e. left outer union right outer
	 */
	private String buildView(String newTableName) {
		
		String buildViewQuery = "CREATE OR REPLACE VIEW "+newTableName + " AS ";
		String selectQuery = "";
		Set<String> colheads = new HashSet<String>();
		
		//this section builds the join part of the select query
		Map<Object, Object> map = joinColsMap.get(newTableName);
		for(Object key : map.keySet()) {
			H2Frame[] frames = (H2Frame[])key;
			Map<String, String> joinCols = (Map<String, String>)map.get(frames);
			
			String table1 = frames[0].builder.getTableName();
			String table2 = frames[1].builder.getTableName();
			
			String[] columnHeaders1 = frames[0].getColumnHeaders();
			String[] columnHeaders2 = frames[1].getColumnHeaders();
			addToSet(colheads, columnHeaders1);
			addToSet(colheads, columnHeaders2);
			
			selectQuery += frames[0].builder.getTableName() + " INNER JOIN " + frames[1].builder.getTableName() + " ON ";
			for(String joinCol1 : joinCols.keySet()) {
				String joinCol2 = joinCols.get(joinCol1);
				selectQuery += table1+"."+joinCol1+ "="+table2+"."+joinCols.get(joinCol1);
				colheads.remove(joinCol1);
				colheads.remove(joinCol2);
				if(joinCol1.equalsIgnoreCase(joinCol2)) {
					colheads.add(table1+"."+joinCol1);
				} else {
					colheads.add(table1+"."+joinCol1+" AS "+joinCol2);
				}
			}
		}
		
		//build the selectors part of the select query
		String selectors = " (SELECT ";
		int i = 0;
		for(String selector : colheads) {
			if(i==0) {
				selectors += selector;
				i++;
			} else {
				selectors += ", "+selector;
			}  
		}
		selectQuery = selectors +" FROM "+ selectQuery+ ")";
		
		
		return buildViewQuery+selectQuery;
	}
	
	private void addToSet(Set<String> set, String[] array) {
		for(String arr : array) {
			set.add(arr);
		}
	}
	
	/**
	 * 
	 * @return		a new unique name that can be used for a view
	 */
	private static String getNewTableName() {
		return tableNamePrefix+counter++;
	}
	
	public void updateDataId(String viewTableName) {
		List<H2Frame> frames = getJoinedFrames();//joinedFrames.get(viewTableName);
		
		if(frames != null) {
			for(H2Frame frame : frames) {
				frame.updateDataId(1);
			}
		}
	}
	
	
	/**************************************** FOR MULTI COLUMN -- DEVELOP THIS GOING FORWARD *************************/
	
	/**
	 * 
	 * @param joinCols
	 * @param frames
	 * 
	 * use this for new multi column join
	 */
	public void joinFrames(List<List<String>> joinCols, H2Frame[] frames) {
		
		//assuming for now we are only joining non joined insights
		for(H2Frame frame : frames) {
			if(frame.isJoined()) {
				throw new IllegalArgumentException("Frame already joined");
			}
		}
		
		String[] tableNames = new String[frames.length];
		for(int i = 0; i < frames.length; i++) {
			tableNames[i] = frames[i].builder.getTableName();
		}
		
		String newTable = getNewTableName();
		List<String[]> newjoinCols = convertJoinColsToJoinVals(frames, joinCols);
		
		List<String> selectors = buildNewSelectors(frames, newjoinCols);
		String buildViewQuery = buildQuery(newTable, newjoinCols, tableNames, selectors);
		try {
			frames[0].builder.runExternalQuery(buildViewQuery);
			for(H2Frame frame : frames) {
				frame.setJoin(newTable);
				frame.builder.setJoiner(this);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private List<String> buildNewSelectors(H2Frame[] frames, List<String[]> joinCols) {

		List<String> selectors = new ArrayList<>();
		
		Map<H2Frame, List<String>> joinMap = getJoinColMap(frames, joinCols);
		
		H2Frame mainFrame = frames[0];
		String[] mainColumnHeaders = getValuesHeadersFromFrame(mainFrame);
		String tableName = mainFrame.builder.getTableName();
		String[] mainViewColumnHeaders = concatHeadersWithTable(tableName, mainColumnHeaders); 
		
		List<String> mainJoins = joinMap.get(mainFrame);
		for(int i = 0; i < mainColumnHeaders.length; i++) {
			selectors.add(tableName+"."+mainColumnHeaders[i]+" as "+mainViewColumnHeaders[i]);
			mainFrame.builder.addTranslation(mainColumnHeaders[i], mainViewColumnHeaders[i]);
			if(mainJoins.contains(mainColumnHeaders[i])) {
				//we are adding a join column to translation
				int index = mainJoins.indexOf(mainColumnHeaders[i]);
				for(int j = 1; j < frames.length; j++) {
					String header = joinMap.get(frames[j]).get(index);
					frames[j].builder.addTranslation(header, mainViewColumnHeaders[i]);				
				}
			}
		}
		
		for(int i = 1; i < frames.length; i++) {
			String[] colHeaders = getValuesHeadersFromFrame(frames[i]);
			String tName = frames[i].builder.getTableName(); 
			List<String> frameJoinCols = joinMap.get(frames[i]);
			for(int j = 0; j < colHeaders.length; j++) {
				if(!frameJoinCols.contains(colHeaders[j])) {
					String viewName = tName+"_"+colHeaders[j];
					selectors.add(tName+"."+colHeaders[j]+" as "+viewName);
					frames[i].builder.addTranslation(colHeaders[j], viewName);
				}
			}			
		}
		
		System.out.println(selectors.toString());
		return selectors;
	}
	
	//makes a map that defines which join columns are associated with which frame
	private Map<H2Frame, List<String>> getJoinColMap(H2Frame[] frames, List<String[]> joinCols) {
		Map<H2Frame, List<String>> map = new HashMap<>();
		
		for(String[] nextJoinCols : joinCols) {
			for(int i = 0; i < nextJoinCols.length; i++) {
				if(map.containsKey(frames[i])) {
					map.get(frames[i]).add(nextJoinCols[i]);
				} else {
					List<String> list = new ArrayList<>();
					list.add(nextJoinCols[i]);
					map.put(frames[i], list);
				}
			}
		}
 		
		return map;
	}
	
	//convert the join columns to use the headers of the actual h2 table
	private List<String[]> convertJoinColsToJoinVals(H2Frame[] frames, List<List<String>> joinCols) {
		List<String[]> joinVals = new ArrayList<>(joinCols.size());
		
//		for(int index = 0; index < joinCols.size(); index++) {
		for(List<String> nextJoinCols : joinCols) {
//			String[] nextJoinCols = joinCols.get(index);
			String[] nextJoinVals = new String[nextJoinCols.size()];
			for(int i = 0; i < nextJoinCols.size(); i++) {
				nextJoinVals[i] = frames[i].getValueForUniqueName(nextJoinCols.get(i));
			}
			joinVals.add(nextJoinVals); 
		}
		
		return joinVals;
	}
	
	//use this method to grab the h2 headers from the frame, we need that since we are querying the table
	private String[] getValuesHeadersFromFrame(H2Frame frame) {
		
		String[] columnHeaders = frame.getColumnHeaders();
		String[] valueHeaders = new String[columnHeaders.length];
		for(int i = 0; i < columnHeaders.length; i++) {
			valueHeaders[i] = frame.getValueForUniqueName(columnHeaders[i]);
		}
		return valueHeaders;
	}
	
	//use this method to turn header into tableName.header, need this to eliminate ambiguity in view building query
	private String[] concatHeadersWithTable(String tableName, String[] columnHeaders) {
		String[] newHeaders = new String[columnHeaders.length];
		for(int i = 0; i < columnHeaders.length; i++) {
			newHeaders[i] = tableName+"_"+columnHeaders[i];
		}
		return newHeaders;
	}
	
	
	/**
	 * @param newTableName
	 *            - name to create view as
	 * 
	 * @param columns
	 *            [[Title, Title, MovieTitle][Director, Director, Director3]]
	 * 
	 * @param tables
	 *            [Frame1, Frame2, Frame3]
	 * 
	 * @return
	 * 
	 * 		Example of return :
	 *         "CREATE VIEW newTableName AS (SELECT Frame1.TITLE, Frame1.DIRECTOR, Genre FROM Frame1 INNER JOIN Frame2 ON Frame1.TITLE = Frame2.TITLE AND Frame1.DIRECTOR = Frame2.Director)"
	 */
	public String buildQuery(String newTableName, List<String[]> columns, String[] tables, List<String> selectors) {
		String table1 = tables[0];
		int tableSize = tables.length;
		int tempTableIndex = 1;

		String query = "CREATE VIEW " + newTableName + " AS (SELECT ";

		for (String sel : selectors) {
			query += sel + ", ";
		}
	    // remove extra chars
		query = query.substring(0, query.length() - 2);
		
		query += " \nFROM " + table1;

		// loop through table list and create additional INNER JOIN
		while (tempTableIndex < tableSize) {
			String tempTable2 = tables[tempTableIndex];
			query += " \nINNER JOIN " + tempTable2 + " ON \n";

			// grab single column from multiple String[]
			Iterator<String[]> ic = columns.iterator();
			String[] tempColArr = null;

			while (ic.hasNext()) {
				tempColArr = ic.next();

				query += table1 + "." + tempColArr[0] + " = " // Constant for table 1
						+ tempTable2 + "." + tempColArr[tempTableIndex] + " AND ";

			}
		    // remove extra chars
			query = query.substring(0, query.length() - 5);

			tempTableIndex++;
		}

		query += ")";

		return query;
	}
	
	/**
	 * 
	 * @param frames		This method unjoins a list of frames
	 * 							if last frame is for a view is unjoined, the view table will also be dropped
	 * 							if the second to last frame for a view, that frame will also be unjoined
	 * 							
	 * 							Any affected views with remaning frames will be refreshed to be reflective of the loss of data 
	 * 			
	 */
	public void unJoinFrame(H2Frame... frames){
		
		//set of views to refresh
		Set<String> viewsToRefresh = new HashSet<String>(frames.length);
		
		//for each frame if the frame is joined
		for(H2Frame frame : frames) {
			if(frame.isJoined()) {
				
				//remove the frame from the frame list
				String viewTable = frame.builder.getViewTableName();
				List<H2Frame> frameList = getJoinedFrames();//joinedFrames.get(viewTable);
				frameList.remove(frame);
				
				
				//add the table to refresh set
				viewsToRefresh.add(viewTable);
				
				//last frame can't be joined to anything so unjoin that as well
				if(frameList.size() == 1) {
					unJoinFrame(frameList.get(0));
				} 
				
				//if no more frames left just drop the view
				else if(frameList.isEmpty()) {
					//drop the view table
					frame.builder.dropView();
//					joinedFrames.remove(viewTable);
				}
				
				//finally call unjoin on the frame letting the frame know it is unjoined
				frame.unJoin();
			}
		}
		
		//for each view refresh if it is still associated with joined frames
		for(String view : viewsToRefresh) {
			List<H2Frame> frameList = getJoinedFrames();//joinedFrames.get(view);
			if(frameList != null) {
				H2Frame frame = frameList.get(0);
				refreshView(frame, view);
			}
		}
	}
}
