package prerna.ds.H2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.ds.H2.H2Builder.Comparator;

/**
 * 
 * 
 *
 */
public class H2Joiner {

	private static int counter = 0;
	private static String tableNamePrefix = "VIEWTABLE94328"; //something random...hopefully no one else makes a table named this
	//this will map the name of a view table to List of H2 frames
	private static Hashtable<String, List<H2Frame>> joinedFrames = new Hashtable<>();
	
	//mapping from 2 frames to joinColumns
	//view table -> {[frame, frame] -> [joinCol, joinCol]}
	private static Hashtable<String, Map<Object, Object>> joinColsMap = new Hashtable<>();
	

	/**
	 * 
	 * @param frame1
	 * @param frame2
	 * @param joinCols - columns to be joined on, keys in map refer to columns in frame1, values refer to columns in frame2
	 */
	public static void joinFrames(H2Frame frame1, H2Frame frame2, Map<String, String> joinCols) {
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
	
	/**
	 * 
	 * @param frames		This method unjoins a list of frames
	 * 							if last frame is for a view is unjoined, the view table will also be dropped
	 * 							if the second to last frame for a view, that frame will also be unjoined
	 * 							
	 * 							Any affected views with remaning frames will be refreshed to be reflective of the loss of data 
	 * 			
	 */
	public static void unJoinFrame(H2Frame... frames){
		
		//set of views to refresh
		Set<String> viewsToRefresh = new HashSet<String>(frames.length);
		
		//for each frame if the frame is joined
		for(H2Frame frame : frames) {
			if(frame.isJoined()) {
				
				//remove the frame from the frame list
				String viewTable = frame.builder.getViewTableName();
				List<H2Frame> frameList = joinedFrames.get(viewTable);
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
					joinedFrames.remove(viewTable);
				}
				
				//finally call unjoin on the frame letting the frame know it is unjoined
				frame.unJoin();
			}
		}
		
		//for each view refresh if it is still associated with joined frames
		for(String view : viewsToRefresh) {
			List<H2Frame> frameList = joinedFrames.get(view);
			if(frameList != null) {
				H2Frame frame = frameList.get(0);
				refreshView(frame, view);
			}
		}
	}
	
	/**
	 * 
	 * @param frame
	 * @param viewTable
	 */
	public static void refreshView(H2Frame frame, String viewTable) {
		try {
			String buildViewQuery = buildView(viewTable);
			frame.builder.runExternalQuery(buildViewQuery);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param tableName			name of the view to get filters for
	 * @return					a merge of all the filter data from the frames associated with the tableName (view) parameter
	 */
	protected static Map<String, Map<Comparator, Set<Object>>> getJoinedFilterHash(String tableName) {
		List<H2Frame> frames = joinedFrames.get(tableName);
		//build the filter query from the frames
		Map<String, Map<Comparator, Set<Object>>> filterHash = new HashMap<>();
		for(H2Frame frame : frames) {
			Map<String, Map<Comparator, Set<Object>>> nextHash = frame.builder.filterHash2;
			for(String Column : nextHash.keySet()) {
				if(filterHash.containsKey(Column)) {
					Map<Comparator, Set<Object>> columnHash = filterHash.get(Column);
					Map<Comparator, Set<Object>> nextColumnHash = nextHash.get(Column);
					for(Comparator comparator : nextColumnHash.keySet()) {
						if(columnHash.containsKey(comparator)) {
							columnHash.get(comparator).addAll(nextColumnHash.get(comparator));
						} else {
							columnHash.put(comparator, nextColumnHash.get(comparator));
						}
					}
				} else {
					filterHash.put(Column, nextHash.get(Column));
				}
			}
		}
		
		return filterHash;
	}
	
	/**
	 * 
	 * @param newTableName 			table name of the view that will be created
	 * @return						The sql query that will build the view
	 * 
	 * Example of return : "CREATE VIEW newTableName AS (SELECT Frame1.TITLE, Director, Genre FROM Frame1 INNER JOIN Frame2 ON Frame1.TITLE = Frame2.TITLE)"
	 */
	private static String buildView(String newTableName) {
		
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
	
	private static void addToSet(Set<String> set, String[] array) {
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
	
	public static void main(String[] args) {
		
	}
}
