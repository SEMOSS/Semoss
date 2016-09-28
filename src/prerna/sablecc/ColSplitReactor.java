package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;

public class ColSplitReactor extends AbstractReactor {

	ITableDataFrame frame;

	public ColSplitReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.WORD_OR_NUM}; // these are the input columns - there is also expr Term which I will come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_SPLIT;

		//setting pkqlMetaData
		String title = "Split a column by delimiter";
		String pkqlCommand = "col.split(c:col1,delimiter);";
		String description = "Splits a column to multiple columns based on delimiter";
		boolean showMenu = true;
		boolean pinned = true;
		super.setPKQLMetaData(title, pkqlCommand, description, showMenu, pinned);
		super.setPKQLMetaDataInput();
	}

	@Override
	public Iterator process() {

		ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
		frame.getEdgeHash();

		Vector<String> columns = (Vector<String>)myStore.get(PKQLEnum.COL_DEF);
		String column = columns.get(0);

		String colSplitBase = column+"_SPLIT_";
		String delimiter = (String)myStore.get(PKQLEnum.WORD_OR_NUM);

		Iterator<Object> colIterator = frame.uniqueValueIterator(column, false);

		int highestIndex = 0;
		//first update table
		while(colIterator.hasNext()) {

			String nextVal = colIterator.next().toString();
			String[] newVals = nextVal.split(delimiter);

			Map<String, Object> newMap = new LinkedHashMap<>();
			newMap.put(column, nextVal);

			if(newVals.length > highestIndex) {
				Map<String, Set<String>> newEdgeHash = new LinkedHashMap<>();
				Set<String> set = new LinkedHashSet<>();
				for(int i = highestIndex; i < newVals.length; i++) {

					set.add(colSplitBase+i);
				}
				newEdgeHash.put(column, set);
				//TODO: empty  hashmap will default types to string, need to also be able to create other type columns
				//		in cases of splitting dates and decimals
				frame.mergeEdgeHash(newEdgeHash, new HashMap<>());
				highestIndex = newVals.length;
			}


			for(int i = 0; i < newVals.length; i++) {
				newMap.put(colSplitBase+i, newVals[i]);
			}

			frame.addRelationship(newMap);	//cleanRow, rawRow		
		}	
		//then update meta data

		//remove column
		//		frame.removeColumn(column);
		frame.updateDataId();
		
		
		return null;
	}

	//////////////setting the values for PKQL JSON for FE//////////////////////
	
	/*private List<HashMap<String, Object>> populatePKQLMetaDataInput(){
		List<HashMap<String, Object>> input = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> inputMap = new HashMap<String, Object>();
		Object restrictions = new Object();

		//first variable in PKQL
		inputMap.put("label", "Column to be split");
		inputMap.put("varName", "c:Col1");
		inputMap.put("dataType", "text");
		inputMap.put("type", "dropdown");
		inputMap.put("restrictions", restrictions);
		inputMap.put("source", "");
		input.add(inputMap);

		//second variable in PKQL
		inputMap = new HashMap<String, Object>();
		inputMap.put("label", "Delimiter");
		inputMap.put("varName", "delimiter");
		inputMap.put("dataType", "text");
		inputMap.put("type", "dropdown");
		inputMap.put("restrictions", restrictions);
		inputMap.put("source", "");
		input.add(inputMap);

		return input;		
	}*/
	
	/*private HashMap<String, Object> populatePKQLMetaDataConsole(){

		HashMap<String, Object> console = new HashMap<String, Object>();
		String[] groups = null;
		Object buttonClass = new Object();
		Object buttonActions = new Object();				

		console.put("name", "Console Name");
		console.put("groups", groups);
		console.put("buttonContentLong", "");
		console.put("buttonContent", "");
		console.put("buttonTitle", "");
		console.put("buttonClass", buttonClass);
		console.put("buttonActions", buttonActions);
		return console;
	}	*/
}
