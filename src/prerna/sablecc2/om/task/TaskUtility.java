package prerna.sablecc2.om.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TaskUtility {

	private TaskUtility() {
		
	}
	
	/**
	 * Flush the task data into an array
	 * This assumes you have table data!!!
	 * @param taskData
	 * @return
	 */
	public static List<Object> flushJobData(ITask taskData) {
		List<Object> flushedOutCol = new ArrayList<Object>();
		// iterate through the task to get the table data
		List<Object[]> data = taskData.flushOutIteratorAsGrid();
		int size = data.size();
		// assumes we are only flushing out the first column
		for(int i = 0; i < size; i++) {
			flushedOutCol.add(data.get(i)[0]);
		}
		
		return flushedOutCol;
	}
	
	/**
	 * We got to predict the type of the values when we have a bunch to merge
	 * @param obj
	 * @return
	 */
	public static PixelDataType predictTypeFromObject(List<Object> obj) {
		int size = obj.size();
		if(size == 0) {
			return PixelDataType.CONST_STRING;
		}
		
		Object firstVal = null;
		int counter = 0;
		while(firstVal == null && counter < size) {
			firstVal = obj.get(counter);
			counter++;
		}
		
		return predictTypeFromObject(firstVal);
	}
	
	/**
	 * We got to predict the type of the values when we have a bunch to merge
	 * @param obj
	 * @return
	 */
	public static PixelDataType predictTypeFromObject(Object obj) {
		if(obj instanceof Double) {
			return PixelDataType.CONST_DECIMAL;
		} else if(obj instanceof Integer) {
			return PixelDataType.CONST_INT;
		} else if(obj instanceof String) {
			return PixelDataType.CONST_STRING;
		} else if(obj instanceof Boolean) {
			return PixelDataType.BOOLEAN;
		}
		
		return PixelDataType.CONST_STRING;
	}
	
	/**
	 * Get a scalar element from collected table data
	 * @param taskData
	 * @return
	 */
	public static NounMetadata getTaskDataScalarElement(Object taskData) {
		//TODO: grab the type from the task data if present instead of doing the cast
		if(taskData instanceof Map) {
			Map dataMap = (Map) ((Map) taskData).get("data");
			if(dataMap != null) {
				List<Object[]> values = (List<Object[]>) dataMap.get("values");
				if(values != null && values.size() == 1) {
					Object[] singleRow = values.get(0);
					if(singleRow.length == 1) {
						Object val = singleRow[0];
						PixelDataType type = predictTypeFromObject(val);
						return new NounMetadata(val, type);
					}
				}
			}
		}
		return null;
	}
	
	/**
	 * See if there is data in the task data output
	 * @param taskData
	 * @return
	 */
	public static boolean noData(Object taskData) {
		//TODO: grab the type from the task data if present instead of doing the cast
		if(taskData instanceof Map) {
			Map dataMap = (Map) ((Map) taskData).get("data");
			if(dataMap != null) {
				List<Object[]> values = (List<Object[]>) dataMap.get("values");
				if(values == null || values.isEmpty()) {
					return true;
				}
			}
		}
		return false;
	}
}
