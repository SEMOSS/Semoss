//package prerna.sablecc;
//
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.sablecc.PKQLRunner.STATUS;
//import prerna.sablecc.meta.DataframeHeaderMetadata;
//import prerna.sablecc.meta.IPkqlMetadata;
//
//public class DataFrameHeaderReactor extends AbstractReactor {
//
//	private static final String NUMERIC_VALUES_ONLY = "onlyNumeric";
//	private static final String SORT_VALUES = "sortDir";
//
//	public DataFrameHeaderReactor() {
//		String[] thisReacts = {PKQLEnum.MAP_OBJ};
//		super.whatIReactTo = thisReacts;
//		super.whoAmI = PKQLEnum.DATA_FRAME_HEADER;
//	}
//
//	@Override
//	public Iterator process() {
//		ITableDataFrame table = (ITableDataFrame) myStore.get("G");
//		
//		// get additional options
//		boolean sort = false;
//		String sortDirection = "";
//		boolean returnOnlyNumbers = false;
//		Map<Object, Object> mapOptions = (Map<Object, Object>) myStore.get(PKQLEnum.MAP_OBJ);
//		if(mapOptions != null) {
//			if(mapOptions.containsKey(NUMERIC_VALUES_ONLY)) {
//				returnOnlyNumbers = Boolean.parseBoolean(mapOptions.get(NUMERIC_VALUES_ONLY) + "");
//			}
//			if(mapOptions.containsKey(SORT_VALUES)) {
//				sort = true;
//				sortDirection = mapOptions.get(SORT_VALUES) + "";
//			}
//		}
//		
//		// get the table header object
//		List<Map<String, Object>> tableHeaderObjects = table.getTableHeaderObjects();
//		int size = tableHeaderObjects.size();
//		// keep track in case we need to remove specific indices
//		boolean[] arrayRemoveIndex = new boolean[size];
//		for(int i = 0; i < size; i++) {
//			Map<String, Object> nextObject = tableHeaderObjects.get(i);
//			Object name = nextObject.remove("varKey");
//			nextObject.remove("uri");
//			nextObject.put("name", name);
//			if(returnOnlyNumbers) {
//				String type = nextObject.get("type").toString();
//				if(!type.equals("NUMBER")) {
//					arrayRemoveIndex[i] = true;
//				}
//			}
//		}
//		
//		// taking into consideration option to only return numerical values
//		if(returnOnlyNumbers) {
//			int offset = 0;
//			for(int i = 0; i < size; i++) {
//				if(arrayRemoveIndex[i] == true) {
//					// need to keep track of offset since 
//					// indices change with each removal
//					tableHeaderObjects.remove(i - offset);
//					offset++;
//				}
//			}
//		}
//		
//		// taking into consideration to sort values alphabetically
//		// default sort is based on order it was added to the frame
//		// TODO: future optimization to auto sort when getting data from meta data
//		if(sort) {
//			Collections.sort(tableHeaderObjects, new HeaderComparable(sortDirection));
//		}
//		
//		myStore.put("tableHeaders", tableHeaderObjects);
//		myStore.put("STATUS", STATUS.SUCCESS);
//
//		return null;
//	}
//
//	public IPkqlMetadata getPkqlMetadata() {
//		DataframeHeaderMetadata metadata = new DataframeHeaderMetadata();
//		metadata.setPkqlStr((String) myStore.get(PKQLEnum.DATA_FRAME_HEADER));
//		return metadata;
//	}
//
//}
//
//class HeaderComparable implements Comparator<Map<String, Object>> {
//
//	private String sortDir;
//	
//	public HeaderComparable(String sortDir) {
//		this.sortDir = sortDir.toLowerCase();
//	}
//	
//	@Override
//	public int compare(Map<String, Object> o1, Map<String, Object> o2) {
//		if(sortDir.startsWith("asc")) {
//			return -1 * o1.get("name").toString().compareTo(o2.get("name").toString());
//		} else {
//			return o1.get("name").toString().compareTo(o2.get("name").toString());
//		}
//	}
//}
//
