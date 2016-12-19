package prerna.sablecc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.meta.ColFilterMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.VizPkqlMetadata;
import prerna.util.Utility;

public class ColFilterReactor extends AbstractReactor {

	// Hashtable <String, String[]> values2SyncHash = new Hashtable <String,
	// String[]>();
	ITableDataFrame frame;

	public ColFilterReactor() {
		String[] thisReacts = { PKQLEnum.FILTER }; // these are the input
													// columns - there is also
													// expr Term which I will
													// come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.FILTER_DATA;

		// setting pkqlMetaData
//		String title = "Filter data in a column";
//		String pkqlCommand = "col.filter(c:col1=[instances]);";
//		String description = "Filters the data in the selected column";
//		boolean showMenu = true;
//		boolean pinned = true;
//		super.setPKQLMetaData(title, pkqlCommand, description, showMenu, pinned);
//		super.setPKQLMetaDataInput();
	}

	@Override
	public Iterator process() {
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String) myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);

		frame = (ITableDataFrame) myStore.get("G");

		Vector<Hashtable> filters = (Vector<Hashtable>) myStore.get(PKQLEnum.FILTER);
		this.processFilters(frame, filters);

		// update the data id so FE knows data has been changed
		frame.updateDataId();

		return null;
	}

	private Map<String, Map<String, List<Object>>> getFilters(Vector<Hashtable> filters) {
		Map<String, Map<String, List<Object>>> fdata = new HashMap<>();

		Map<String, String> properties = new HashMap<>();
		if (frame != null) {
			properties = frame.getProperties();
		}

		for (int filterIndex = 0; filterIndex < filters.size(); filterIndex++) {
			Hashtable thisFilter = (Hashtable) filters.get(filterIndex);
			String fromCol = (String) thisFilter.get("FROM_COL");
			Vector filterData = new Vector();

			if (!thisFilter.containsKey("TO_COL")) {
				filterData = (Vector) thisFilter.get("TO_DATA");
				List<Object> cleanedFilterData = new ArrayList<>(filterData.size());
				for (Object data : filterData) {
					String inputData = data.toString().trim();
					Object cleanData = null;
					if(data.equals(AbstractTableDataFrame.VALUE.NULL)) {
						cleanData = data;
					}

					// if the column type in the frame is a string and a
					// property simply cast it to a string
					if (frame != null && frame.getDataType(fromCol).equals(IMetaData.DATA_TYPES.STRING)
							&& properties.containsKey(fromCol) && cleanData == null) {
						cleanData = inputData;
					}

					else if (frame != null && frame.getDataType(fromCol).equals(IMetaData.DATA_TYPES.STRING)
							&& !properties.containsKey(fromCol) && cleanData == null) {
						cleanData = Utility.cleanString(inputData, true, true, false);
					}

					// else go through the current flow
					// TODO : we should use the types on the frame to determine
					// how to cast instead of guessing what came from pkql
					else if(cleanData == null) {
						String type = Utility.findTypes(inputData)[0] + "";
						if (type.equalsIgnoreCase("Date")) {
							cleanData = Utility.getDate(inputData);
						} else if (type.equalsIgnoreCase("Double")) {
							cleanData = Utility.getDouble(inputData);
						} else {
							if (properties.containsKey(fromCol)) {
								// if we are filtering a property, don't clean
								cleanData = inputData;
							} else {
								cleanData = Utility.cleanString(inputData, true, true, false);
							}
						}
					}

					cleanedFilterData.add(cleanData);
				}

				String comparator = (String) thisFilter.get("COMPARATOR");
				if (fdata.containsKey(fromCol)) {
					Map<String, List<Object>> innerMap = fdata.get(fromCol);
					if (innerMap.containsKey(comparator)) {
						List<Object> existingFilterData = innerMap.get(comparator);
						existingFilterData.addAll(cleanedFilterData);
						innerMap.put(comparator, existingFilterData);
					} else {
						innerMap.put(comparator, cleanedFilterData);
					}
				} else {
					Map<String, List<Object>> innerMap = new HashMap<>();
					innerMap.put(comparator, cleanedFilterData);
					fdata.put(fromCol, innerMap);
				}
			}
		}
		return fdata;
	}

	private void processFilters(ITableDataFrame frame, Vector<Hashtable> filters) {
		Map<String, Map<String, List<Object>>> processedFilters = getFilters(filters);
		for (String columnHeader : processedFilters.keySet()) {
			try {
				if(frame instanceof H2Frame) {
					if(((H2Frame)frame).isJoined()) {
						((H2Frame) frame).getJoiner().filter(frame, columnHeader, processedFilters.get(columnHeader));
					} else {
						frame.filter(columnHeader, processedFilters.get(columnHeader));
					}
				} else {
					frame.filter(columnHeader, processedFilters.get(columnHeader));
				}
				myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
				myStore.put("FILTER_RESPONSE", "Filtered Column: " + columnHeader);
			} catch (IllegalArgumentException e) {
				myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
				myStore.put("FILTER_RESPONSE", e.getMessage());
			}
		}
	}

	public IPkqlMetadata getPkqlMetadata() {
		Vector filters = (Vector) myStore.get(PKQLEnum.FILTER);
		Hashtable filterHT = (Hashtable) filters.get(0);
		String filterStr = (String) filterHT.get(PKQLEnum.FROM_COL);
		filterStr += (String) filterHT.get(PKQLEnum.COMPARATOR);
		filterStr += (String) filterHT.get("TO_DATA").toString();
		ColFilterMetadata metadata = new ColFilterMetadata(filterStr);
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.FILTER_DATA));
		return metadata;
	}
	// private void processFilters(ITableDataFrame frame, Vector<Hashtable>
	// filters) {
	//
	// for(int filterIndex = 0;filterIndex < filters.size();filterIndex++)
	// {
	// Object thisObject = filters.get(filterIndex);
	//
	// Hashtable thisFilter = (Hashtable)filters.get(filterIndex);
	// String fromCol = (String)thisFilter.get("FROM_COL");
	// String toCol = null;
	// Vector filterData = new Vector();
	// if(thisFilter.containsKey("TO_COL")) {
	// toCol = (String)thisFilter.get("TO_COL");
	// }
	// else {
	// filterData = (Vector)thisFilter.get("TO_DATA");
	// List<Object> cleanedFilterData = new ArrayList<>(filterData.size());
	// for(Object data : filterData) {
	// String inputData = data.toString().trim();
	// Object cleanData = null;
	// // grammar change should no longer allow for quotes to be added due to
	// outAWord change
	//// if((inputData.startsWith("\"") && inputData.endsWith("\"")) ||
	// (inputData.startsWith("'") && inputData.endsWith("'"))) {
	//// // this is logic that input is a string
	//// inputData = inputData.substring(1, inputData.length() - 1);
	//// cleanData = Utility.cleanString(inputData, true, true, false);
	//// } else {
	// String type = Utility.findTypes(inputData)[0] + "";
	// if(type.equalsIgnoreCase("Date")) {
	// cleanData = Utility.getDate(inputData);
	// } else if(type.equalsIgnoreCase("Double")) {
	// cleanData = Utility.getDouble(inputData);
	// } else {
	// cleanData = Utility.cleanString(inputData, true, true, false);
	// }
	//// }
	// cleanedFilterData.add(cleanData);
	// }
	// String comparator = (String)thisFilter.get("COMPARATOR");
	// try {
	// frame.filter(fromCol, cleanedFilterData, comparator);
	// myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
	// myStore.put("FILTER_RESPONSE", "Filtered Column: " + fromCol);
	// } catch(IllegalArgumentException e) {
	// myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
	// myStore.put("FILTER_RESPONSE", e.getMessage());
	// }
	// }
	// }
	// }
}
