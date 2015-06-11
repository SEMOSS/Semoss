package prerna.rdf.main;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.error.FileReaderException;
import prerna.nameserver.MasterDatabaseQueries;
import prerna.nameserver.MasterDatabaseURIs;
import prerna.util.ArrayListUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DataStructureFromTable {

	private ArrayList<Object[]> table = new ArrayList<Object[]>();
	private String[] headers;
	private String[] columnTypes;

	private int concatThreshold;
	
	private boolean[] colAlreadyProperty;
	
	private Hashtable<String, List<String>> matchedColsHash;
	private ArrayList<String[]> matchesList;
	
	private final String RELATION_UNKNOWN = "Relationship is TBD";
	private final String RELATION_CONCAT = "Has";

	private String masterDBName = "LocalMasterDatabase";
	private BigDataEngine masterEngine;

	public void setMasterDB(String localMasterDbName) {
		this.masterDBName = localMasterDbName;
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
	}

	public void setMasterDB() {
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
	}
	
	public void setConcatThreshold(int concatThreshold) {
		this.concatThreshold = concatThreshold;
	}
	
	public void createMetamodel(String fileLoc) {
		try {
			readCSVFile(fileLoc);
		} catch (FileReaderException e) {
			e.printStackTrace();
			return;
		}

		long startT = System.currentTimeMillis();
		
		matchedColsHash = new Hashtable<String, List<String>>();
		matchesList = new ArrayList<String[]>();
		
		//ordering the engines by the ones that contain the most number of columns 
		List<String> orderedEnginesList = prioritizeDBs();
		
		//process through the ordered engines to add any exact matches from master db.
		System.out.println("\nFinding relationships in Master DB that are exact matches to columns... ");
		findExactMatchRelationshipsInMasterDB(orderedEnginesList);
		
		//find matches based upon the unique instances
		System.out.println("\nFinding relationships based upon unique identifier analysis... ");
		findRelationshipsBasedOnUniqueIdentifier();
		
		System.out.println("\nResults!!!");
		for(String[] match : matchesList) {
			System.out.println(match[0] + "..." + match[1] + "..." + match[2]);
		}
		
		long endT = System.currentTimeMillis();
		System.out.println("\nTime(s) to run algorithm (excluding reading the excel) = " + (endT - startT)/1000);
	}
	
	/**
	 * Prioritizing the dbs based on the number of columns they support. ones that support more, have lower indicies
	 * @param matchHash
	 */
	private List<String> prioritizeDBs() {

		String bindingsStr = "";
		for(int i=0; i<headers.length; i++) {
			bindingsStr = bindingsStr.concat("(<").concat(MasterDatabaseURIs.KEYWORD_BASE_URI).concat("/").concat(headers[i]).concat(">)");
		}
			
		List<String> orderedEngineList = new ArrayList<String>();
		String query = MasterDatabaseQueries.GET_NUM_KEYWORDS_ENGINE_INCLUDES.replace("@BINDINGS@", bindingsStr);

		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String numKeywords = sjss.getRawVar(names[1]).toString();
			
			orderedEngineList.add(engine);
		}
		return orderedEngineList;
	}
	
	private void findExactMatchRelationshipsInMasterDB(List<String> orderedEngineList) {

		for(String engine : orderedEngineList) {

			String query = MasterDatabaseQueries.GET_ALL_RELATIONSHIPS_FOR_ENGINE.replace("@ENGINE@", engine);
			
			ISelectWrapper wrapper = Utility.processQuery(masterEngine,query);
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext()) {
				
				ISelectStatement sjss = wrapper.next();
				
				String relationship = sjss.getVar(names[0]).toString();
				String vertOne = sjss.getVar(names[1]).toString();
				String vertTwo = sjss.getVar(names[2]).toString();

				int vertOneIndex = ArrayUtilityMethods.calculateIndexOfArray(headers, vertOne);
				int vertTwoIndex = ArrayUtilityMethods.calculateIndexOfArray(headers, vertTwo);
				
				//filter to only include relationships between the columns in my headers array
				if(vertOneIndex > -1 && vertTwoIndex > -1) {
					
					String[] relationshipArr = new String[]{vertOne,relationship,vertTwo};
					
					//if colOne hasnt been matched to anything, just add
					if(!matchedColsHash.containsKey(vertOne)) {
						
						List<String> matchedCols = new ArrayList<String>();
						matchedCols.add(vertTwo);
						matchedColsHash.put(vertOne, matchedCols);
						
						matchesList.add(relationshipArr);
						System.out.println("Found " + vertOne + "..." + relationship + "..." + vertTwo + " in " + engine);
					}
					//if colOne has been matched but not to colTwo, add
					else if(!matchedColsHash.get(vertOne).contains(vertTwo)) {
						List<String> matchedCols = matchedColsHash.get(vertOne);
						matchedCols.add(vertTwo);
						matchedColsHash.put(vertOne, matchedCols);
						
						matchesList.add(relationshipArr);
						System.out.println("Found " + vertOne + "..." + relationship + "..." + vertTwo + " in " + engine);
					}
				}
			}
		}
	}

	
	private void findRelationshipsBasedOnUniqueIdentifier() {

		//determine the column types for the data
		columnTypes = determineColumnTypes(table);
		//determine how many unique instances there are for each column
		int[] numUniqueInstanceArr = getUniqueCounts(table);

		int[] colIndexMinToMax = orderColsByUniqueInstances(numUniqueInstanceArr);
		int numCol = headers.length;
		
		//keep track of the columns that have already been used as properties.
		//cannot use the same column as a property for multiple nodes
		colAlreadyProperty = new boolean[numCol];
		
		//iterating through all string/integer columns
		//going through in min to max order, but need to use the original column index
		for(int i = 0; i < numCol; i++) {
			int colIndex = colIndexMinToMax[i];
			String colName = headers[colIndex];
			
			if(columnTypes[colIndex].equals("STRING")) {
				runAllComparisons(colIndexMinToMax, colName, colIndex);
			}
		}
		
		List<Integer> notUsedList = new ArrayList<Integer>();
		for(int i = 0; i < numCol; i++) {
			if(!colAlreadyProperty[i])
				notUsedList.add(i);
		}
		
		//do the hierarchical part for numbers
		for(int i = 0; i < numCol; i++) {
			int colIndex = colIndexMinToMax[i];
			if(!colAlreadyProperty[colIndex]&&!columnTypes[colIndex].equals("STRING")) {
				List<Integer> parentConcatIndicies = findHierarchicalParent(colIndex, deepCopy(notUsedList));
				if(!parentConcatIndicies.isEmpty()) {
					addHierarchicalParent(colIndex, notUsedList, parentConcatIndicies);
					if(notUsedList.contains(colIndex)) {
						notUsedList.remove(notUsedList.indexOf(colIndex));
					}
				}
			}
		}
		
		
		//iterating through all numerical columns (that have not already been classified as properties) as if they are concepts
		for(int i = 0; i < numCol; i++) {
			//going through in min to max order, but need to use the original column index
			int colIndex = colIndexMinToMax[i];
			String colName = headers[colIndex];
			
			if(!columnTypes[colIndex].equals("STRING")) {
//			if(columnTypes[colIndex].equals("INTEGER") || columnTypes[colIndex].equals("DOUBLE") || columnTypes[colIndex].equals("DATE") || columnTypes[colIndex].equals("SIMPLEDATE")) {
				if(!colAlreadyProperty[colIndex]) {
					runAllComparisons(colIndexMinToMax, colName, colIndex);
				}
			}
		}
		
		notUsedList = new ArrayList<Integer>();
		for(int i = 0; i < numCol; i++) {
			if(!colAlreadyProperty[i])
				notUsedList.add(i);
		}
		
		//do the hierarchical part for strings
		for(int i = 0; i < numCol; i++) {
			int colIndex = colIndexMinToMax[i];
			if(!colAlreadyProperty[colIndex]&&columnTypes[colIndex].equals("STRING")) {
				List<Integer> parentConcatIndicies = findHierarchicalParent(colIndex, deepCopy(notUsedList));
				if(!parentConcatIndicies.isEmpty()) {
					addHierarchicalParent(colIndex, notUsedList, parentConcatIndicies);
					if(notUsedList.contains(colIndex)) {
						notUsedList.remove(notUsedList.indexOf(colIndex));
					}
				}
			}
		}

	}

	private void runAllComparisons(int[] colIndexMinToMax, String firstColName,int firstColIndex) {
		int j;
        int numCol = headers.length;
        for(j = 0; j < numCol; j++) {
 	
			int secondColIndex = colIndexMinToMax[j];
			String secondColName = headers[secondColIndex];

            // dont compare column to itself
            if(firstColIndex != secondColIndex) {
    			// need to make sure second column does not have first column as a property already and second column is not a property of something else
            	if((!matchedColsHash.containsKey(secondColName) || !matchedColsHash.get(secondColName).contains(firstColName)) && (!matchedColsHash.containsKey(firstColName) || !matchedColsHash.get(firstColName).contains(secondColName))) {
                    if(!colAlreadyProperty[secondColIndex] && compareCols(firstColIndex, secondColIndex)) {
                        // we have a match!!!
                        boolean useInverse = false;
                        if(firstColIndex > secondColIndex) {
                            // try to see if inverse order is better
                            // but first, check to make sure the second column is not a double/date
                            if(columnTypes[secondColIndex].equals("STRING")) {
                                if(!colAlreadyProperty[firstColIndex] && compareCols(secondColIndex, firstColIndex)) {
                                    // use reverse order
                                    useInverse = true;
                                    
                                    if(!matchedColsHash.containsKey(secondColName)) {
                                    	matchedColsHash.put(secondColName,new ArrayList<String>());
                                    }
                                    matchedColsHash.get(secondColName).add(firstColName);
                                	String[] matchArr = new String[]{secondColName,RELATION_UNKNOWN,firstColName};
                                	matchesList.add(matchArr);

                                    colAlreadyProperty[firstColIndex] = true;
                                }
                            }   
                        }
                        if(!useInverse) {
                        	if(!matchedColsHash.containsKey(firstColName)) {
                        		matchedColsHash.put(firstColName,new ArrayList<String>());
                            }
                        	matchedColsHash.get(firstColName).add(secondColName);
                            String[] matchArr = new String[]{firstColName,RELATION_UNKNOWN,secondColName};
                        	matchesList.add(matchArr);
                            
                            colAlreadyProperty[secondColIndex] = true;
                        }
                    }
                }
            }
        }
	}
	
	private List<Integer> findHierarchicalParent(int childColIndex, List<Integer> notUsedList) {

		Object[] childCol = ArrayListUtilityMethods.getColumnFromList(table,childColIndex);

		notUsedList.remove(notUsedList.indexOf(childColIndex));
		int numColsInConcat = 2;
		while(numColsInConcat <= concatThreshold){
			List<Integer> currColConcat = new ArrayList<Integer>();
			List<List<Integer>> colConcatList = getColConcatList(currColConcat, numColsInConcat, notUsedList);
			
			for(List<Integer> colConcat : colConcatList) {
				
				//make the concated columns
				Object[] possibleParentCol = new String[table.size()];
				for(int rowIndex = 0; rowIndex < table.size(); rowIndex++) {
					Object[] row = table.get(rowIndex);
					String concat = "";
					for(int colIndex = 0; colIndex < colConcat.size(); colIndex++) {
						concat = concat.concat(row[colConcat.get(colIndex)].toString()).concat("~");
					}
					possibleParentCol[rowIndex] = concat;
				}
				
				//see if the concatenation is a parent
				if(compareCols(possibleParentCol,childCol)) {
					return colConcat;
				}
			}
			numColsInConcat ++;
		}
		
		return new ArrayList<Integer>();
	}
	
	
	private void addHierarchicalParent(int colIndex, List<Integer> notUsedList, List<Integer> parentConcatIndicies){
		//TODO what if there are multiple concats for a given size.
		String concat = "";
		List<String> parentConcatNames = new ArrayList<String>();
		for(Integer index: parentConcatIndicies) {
			String parent = headers[index];			
            concat = concat.concat(parent);
            parentConcatNames.add(parent);

            if(!matchedColsHash.containsKey(parent)) {
            	matchedColsHash.put(parent,new ArrayList<String>());
            }
            matchedColsHash.get(parent).add(headers[colIndex]);
		}

		if(!matchedColsHash.containsKey(concat)) {
         	matchedColsHash.put(concat,parentConcatNames);
         	for(String parent : parentConcatNames) {
            	String[] matchArr = new String[]{concat,RELATION_CONCAT,parent};
            	matchesList.add(matchArr);
         	}
        }
		
    	String[] matchArr = new String[]{concat,RELATION_UNKNOWN,headers[colIndex]};
    	matchesList.add(matchArr);
		colAlreadyProperty[colIndex] = true;

	}
	
	//make a list of all the possible col concats for this size
	private List<List<Integer>> getColConcatList(List<Integer> currColConcat, int maxNumColsInConcat, List<Integer> possibleColsList) {

		List<List<Integer>> ret = new ArrayList<List<Integer>>();
		if(currColConcat.size() == maxNumColsInConcat) {
			ret.add(deepCopy(currColConcat));
			return ret;
		}
		
		int latestColIndex;
		if(currColConcat.isEmpty()) {
			latestColIndex = -1;
		}else {
			int latestCol = currColConcat.get(currColConcat.size()-1);
			latestColIndex = possibleColsList.indexOf(latestCol);
		}
		
		if(latestColIndex == possibleColsList.size() - 1) {
			return ret;
		}
		
		while(latestColIndex < possibleColsList.size()-1) {
			currColConcat.add(possibleColsList.get(latestColIndex + 1));
			ret.addAll(getColConcatList(currColConcat,maxNumColsInConcat, possibleColsList));
			currColConcat.remove(possibleColsList.get(latestColIndex + 1));
			latestColIndex++;
		}
		return ret;
	}
	
	private List<Integer> deepCopy(List<Integer> list) {
		List<Integer> copy = new ArrayList<Integer>();
		for(Integer ele : list) {
			copy.add(ele);
		}
		return copy;
	}
	
	private int[] getUniqueCounts(ArrayList<Object[]> data) {
		List<HashSet<Object>> colValues = new ArrayList<HashSet<Object>>();

		int numRows = data.size();
		int numCols = data.get(0).length;
		
		for(int col = 0; col < numCols; col++) {
			colValues.add(new HashSet<Object>());
		}
		
		for(int i = 0; i < numRows; i++) {
			Object[] row = data.get(i);
			for(int j = 0; j < numCols; j++) {
				colValues.get(j).add(row[j]);
			}
		}

		int[] uniqueCounts = new int[numCols];
		for(int col = 0; col < numCols; col++) {
			uniqueCounts[col] = colValues.get(col).size();
		}

		return uniqueCounts;	
	}
	
	private String[] determineColumnTypes(ArrayList<Object []> list) {
		int numCols = list.get(0).length;
		String[] columnTypes = new String[numCols];
		//iterate through columns
		for(int j = 0; j < numCols; j++) {
			columnTypes[j] = determineColumnType(list,j);
		}
		return columnTypes;
	}
	
	private String determineColumnType(ArrayList<Object []> list,int column) {
		//iterate through rows
		int numCategorical = 0;
		int numDouble = 0;
		int numInteger = 0;
		int numDate = 0;
		int numSimpleDate = 0;
		String type;
		for(int i = 0; i < list.size(); i++) {
			Object[] dataRow = list.get(i);
			if(dataRow[column] != null && !dataRow[column].toString().equals("")) {
				String colEntryAsString = dataRow[column].toString();
				if(!colEntryAsString.isEmpty()) {
					type = Utility.processType(colEntryAsString);
					if(type.equals("STRING")) {
						numCategorical++;
					}else if(type.equals("DOUBLE")) {
						numDouble++;
					} else if(type.equals("INTEGER")) {
						numInteger++;
					} else if(type.equals("DATE")) {
						numDate++;
					}else {
						numSimpleDate++;
					}
				}
			}
		}
		if(numDouble > numCategorical && numDouble > numInteger && numDouble > numDate && numDouble > numSimpleDate ) {
			return "DOUBLE";
		} else if(numInteger > numCategorical && numInteger > numDouble && numInteger > numDate && numInteger > numSimpleDate ) {
			return "INTEGER";
		} else if(numDate > numCategorical && numDate > numDouble && numDate > numInteger && numDate > numSimpleDate ) {
			return "DATE";
		} else if(numSimpleDate > numCategorical && numSimpleDate > numDouble && numSimpleDate > numDate && numSimpleDate > numInteger ) {
			return "SIMPLEDATE";
		} else {
			return "STRING";
		}
	}
	
	private boolean compareCols(Object[] firstCol,Object[] secondCol) {
		Hashtable<String, String> values = new Hashtable<String, String>();
		int i=0;
		
        for(; i < table.size(); i++) {
        	Object obj1 = firstCol[i];
        	Object obj2 = secondCol[i];
        	
        	if(obj2 != null) {
	        	if(obj1 == null) {
	        		return false;
	        	}
	        	if(obj1!=null) {
		        	String ele1 = firstCol[i].toString();
		        	String ele2 = secondCol[i].toString();
		        	if(values.containsKey(ele1)) {
		                 if(!values.get(ele1).equals(ele2)) {
		                     // console.log("Not unique for columns: " + mainColName + " and " + otherColName);
		                     // console.log("Not unique for " + ele1 + ": has two values = " + values[ele1] + " && " + ele2);
		                     return false;
		                 } // else do nothing 
		             } else {
		            	 values.put(ele1, ele2);
		             }
	        	}
        	}
         }
         return true;
     }
	
	private boolean compareCols(int firstColIndex,int secondColIndex) {
		Hashtable<String, String> values = new Hashtable<String, String>();
		int i=0;
		
        for(; i < table.size(); i++) {
        	Object obj1 = table.get(i)[firstColIndex];
        	Object obj2 = table.get(i)[secondColIndex];
        	
        	if(obj2 != null) {
	        	if(obj1 == null) {
	        		return false;
	        	}
	        	if(obj1!=null) {
		        	String ele1 = table.get(i)[firstColIndex].toString();
		        	String ele2 = table.get(i)[secondColIndex].toString();
		        	if(values.containsKey(ele1)) {
		                 if(!values.get(ele1).equals(ele2)) {
		                     // console.log("Not unique for columns: " + mainColName + " and " + otherColName);
		                     // console.log("Not unique for " + ele1 + ": has two values = " + values[ele1] + " && " + ele2);
		                     return false;
		                 } // else do nothing 
		             } else {
		            	 values.put(ele1, ele2);
		             }
	        	}
        	}
         }
         return true;
     }
	
	
	private int[] orderColsByUniqueInstances(int[] numUniqueInstanceArr) {
		//order columns by their number of unique instances. Cols with least unique have lower indicies.
		int i = 0;
		int numCol = headers.length;
		int[] colIndexMinToMax = new int[numCol];
		for(; i < numCol; i++) {
			colIndexMinToMax[i] = i;
		}
		
		boolean change = true;
		while(change) {
			change = false;
			for(i = 0; i < numCol-1; i++) {
				if(numUniqueInstanceArr[colIndexMinToMax[i]] > numUniqueInstanceArr[colIndexMinToMax[i+1]]) {
					change = true;
					int valTemp = colIndexMinToMax[i];
					colIndexMinToMax[i] = colIndexMinToMax[i+1];
					colIndexMinToMax[i+1] = valTemp;
				}
			}
		}

		System.out.println("\nNumber of unique instances for each column");
		System.out.println(Arrays.toString(headers));
		System.out.println(Arrays.toString(numUniqueInstanceArr));

		System.out.println("Reordered headers based on number of unique instances");
		for(i = 0; i < numCol; i++) {
			System.out.print(headers[colIndexMinToMax[i]] + ", ");
		}
		System.out.println("");
		
		return colIndexMinToMax;
	}

	private void readCSVFile(String loc) throws FileReaderException {

		ICsvListReader listReader = null;
		try {
			listReader = new CsvListReader(new FileReader(loc), CsvPreference.STANDARD_PREFERENCE);

			if(listReader.read() != null) {
				
				int numCols = listReader.length();
				
				CellProcessor[] processors = new CellProcessor[numCols];
				for(int i=0; i<numCols; i++) {
					processors[i] = new Optional();
				}
				
				List<Object> headerList = listReader.executeProcessors(processors);
				headers = new String[numCols];
				for(int i=0; i<numCols; i++) {
					headers[i] = Utility.cleanString(headerList.get(i).toString(), true);
				}
				
				Set<String> tableSet = new HashSet<String>();
				List<Object> rowList;
				while( (rowList = listReader.read(processors)) != null ) {
					int size = rowList.size();
					String concat = "";
					for(int col = 0; col < size; col++) {
						if(rowList.get(col) == null) {
							rowList.set(col, "");
						}
						concat = concat.concat(rowList.get(col).toString()).concat("~");
					}
					if(!tableSet.contains(concat)) {
						tableSet.add(concat);
						table.add(rowList.toArray());
					}
//					else {
//						//to print the duplicates in the dataset
//						System.out.println(concat);
//					}
				}

			}

		}catch(FileNotFoundException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not find CSV file located at " + loc);			
		} catch (IOException e) {
			e.printStackTrace();
			throw new FileReaderException("Could not read CSV file located at" + loc);	
		}finally {
			try{
				if(listReader!=null) {
					listReader.close();
				}
			}catch(IOException e) {
				e.printStackTrace();
				throw new FileReaderException("Could not close the CSV reader");	
			}
		}
	}
//	
//	private void findHierarchicalStructure() {
//		//determine the column types for the data
//		columnTypes = determineColumnTypes(table);
//		int[] numUniqueInstanceArr = getUniqueCounts(table);
//		List<Set<String>> colConcatResultSet = buildColumnConcatSet();
//		
//		if(colConcatResultSet.isEmpty()) {
//			return;
//		}
//		
//		Set<String> colConcatSet = chooseBestColumnConcat(colConcatResultSet);
//
//		Iterator<String> colNameItr = colConcatSet.iterator();
//		while(colNameItr.hasNext()) {
//			String colName = colNameItr.next();
//			if(matchedColsHash.containsKey(colName)) {
//				List<String> matchList = matchedColsHash.get(colName);
//				Iterator<String> matchItr = matchList.iterator();
//				while(matchItr.hasNext()) {
//					String match = matchItr.next();
//					if(colConcatSet.contains(match)) {
//						return;
//					}
//				}
//			}
//		}
//		
//		//order the columns by increasing indicies
//		int numCols = colConcatSet.size();
//		Integer[] orderedColConcatIndicies = new Integer[numCols];
//		
//		int i=0;
//		colNameItr = colConcatSet.iterator();
//		while(colNameItr.hasNext()) {
//			String colName = colNameItr.next();
//			orderedColConcatIndicies[i] = ArrayUtilityMethods.calculateIndexOfArray(headers, colName);
//			i++;
//		}
//		
//		boolean change = true;
//		while(change) {
//			change = false;
//			for(i = 0; i < numCols-1; i++) {
//				if(numUniqueInstanceArr[orderedColConcatIndicies[i]] < numUniqueInstanceArr[orderedColConcatIndicies[i+1]]) {
//					change = true;
//					int valTemp = orderedColConcatIndicies[i];
//					orderedColConcatIndicies[i] = orderedColConcatIndicies[i+1];
//					orderedColConcatIndicies[i+1] = valTemp;
//				}
//			}
//		}
//		
//		//create the concatenation header
//		String concatColName = "";
//		for(i = 0; i < numCols; i++) {
//			concatColName = concatColName.concat(headers[orderedColConcatIndicies[i]].toString()).concat(":");
//		}
//		concatColName = concatColName.substring(0,concatColName.length() - 1);
//		
//		int numOldCols = headers.length;
//		String[] newHeaders = new String[numOldCols + 1];
//		for(i = 0; i < numOldCols; i++) {
//			newHeaders[i] = headers[i];
//		}
//		newHeaders[numOldCols] = concatColName;
//		headers = newHeaders;
//		
//		//update table with concatenated column
//		for(int row = 0; row < table.size(); row ++) {
//			Object[] oldRow = table.get(row);
//			Object[] newRow = new Object[numOldCols + 1];
//			String concatInstance = "";
//			for(i = 0; i < numCols; i++) {
//				concatInstance = concatInstance.concat(oldRow[orderedColConcatIndicies[i]].toString()).concat(":");
//			}
//			concatInstance = concatInstance.substring(0,concatInstance.length() - 1);
//			
//			for(i = 0; i < numOldCols; i++) {
//				newRow[i] = oldRow[i];
//			}
//			newRow[numOldCols] = concatInstance;
//			table.set(row,newRow);
//		}
//		
//		System.out.println("\nUnique identifier :"+concatColName);
//
//	}
//	
//	private List<Set<String>> buildColumnConcatSet() {
//		
//		//else block of algorithm
//		Set<String> colConcat = buildColumnConcat(headers, table);
//		List<Set<String>> colConcatResultSet = new ArrayList<Set<String>>();
//		if(colConcat.isEmpty())
//			return new ArrayList<Set<String>>();
//		
//		colConcatResultSet.add(colConcat);
//		
//		//if block of algorithm
//		for(int i = 0; i < colConcatResultSet.size(); i++)  {
//			Iterator<String> colNameItr = colConcatResultSet.get(i).iterator();
//			while(colNameItr.hasNext()) {
//				String colName = colNameItr.next();
//				int index = ArrayUtilityMethods.calculateIndexOfArray(headers, colName);
//				ArrayList<Object[]> filteredData = ArrayListUtilityMethods.removeColumnFromList(table, index);
//				String[] filteredHeaders = ArrayUtilityMethods.removeNameFromList(headers, index);
//				Set<String> newColConcat = buildColumnConcat(filteredHeaders,filteredData);
//				if(!newColConcat.isEmpty() && !colConcatResultSet.contains(newColConcat)) {
//					colConcatResultSet.add(newColConcat);
//				}
//			}
//		}
//		
//		return colConcatResultSet;
//		
//	}
//	
//	private Set<String> chooseBestColumnConcat(List<Set<String>> colConcatSet) {
//		//best identifier is the one with least number of columns needed
//		List<Set<String>> tiedColConcats = new ArrayList<Set<String>>();
//		int minSize = headers.length;
//
//		for(Set<String> colConcat : colConcatSet) {
//			if(colConcat.size() < minSize) {
//				tiedColConcats = new ArrayList<Set<String>>();
//				tiedColConcats.add(colConcat);
//				minSize = colConcat.size();
//			}else if(colConcat.size() == minSize) {
//				tiedColConcats.add(colConcat);
//			}
//		}
//		
//		if(tiedColConcats.size() == 1)
//			return tiedColConcats.get(0);
//		
//		//choose the unique identifier that has the lowest index
//		int minSum = headers.length * headers.length;
//		int minIndex = -1;
//		for(int i = 0; i < tiedColConcats.size(); i++) {
//			Iterator<String> colNameItr = tiedColConcats.get(i).iterator();
//			int currSum = 0;
//			while(colNameItr.hasNext()) {
//				String colName = colNameItr.next();
//				currSum += ArrayUtilityMethods.calculateIndexOfArray(headers, colName);
//			}
//			if(currSum < minSum) {
//				currSum = minSum;
//				minIndex = i;
//			}
//		}
//		
//		return tiedColConcats.get(minIndex);
//		
//	}
//
//	private Set<String> buildColumnConcat(String[] headers, ArrayList<Object[]> data) {
//	
//		int numHeaders = headers.length;
//		Set<String> colConcat = new HashSet<String>();
//		if(!isUniqueDataSet(data))
//			return colConcat;
//		
//		int index = 0;
//		for(int i = 0; i < numHeaders; i++) {
////			if(columnTypes[i].equals("STRING") || columnTypes[i].equals("INTEGER")) {
//				ArrayList<Object []> filteredData = ArrayListUtilityMethods.removeColumnFromList(data, index);
//				
//				//if the data is no longer unique, must keep this column and continue
////				if(columnTypes[i].equals("STRING") && !isUniqueDataSet(filteredData)) {
//				if(!isUniqueDataSet(filteredData)) {
//					colConcat.add(headers[i]);
//					index++;
//				}else {//if data is still unique, then remove column
//					data = filteredData;
//				}
////			}
//			if(colConcat.size() > concatThreshold)
//				return new HashSet<String>();
//		}
//		
//		return colConcat;
//	}
//
//	private boolean isUniqueDataSet(ArrayList<Object[]> data) {
//		Set<String> rowConcatSet = new HashSet<String>();
//
//		int numRows = data.size();
//		int numCols = data.get(0).length;
//		for(int row = 0; row < numRows; row ++) {
//			String concat = "";
//			for(int col = 0; col < numCols; col++) {
//				if(columnTypes[col].equals("STRING")) {
//					concat = concat.concat(data.get(row)[col].toString()).concat("~");
//				}
//			}
//			rowConcatSet.add(concat);
//		}
//		if(rowConcatSet.size() < data.size())
//			return false;
//		return true;	
//	}

}
