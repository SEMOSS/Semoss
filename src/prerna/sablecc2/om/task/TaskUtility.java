package prerna.sablecc2.om.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.reactor.task.lambda.map.MapLambdaReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TaskUtility {

	private TaskUtility() {
		
	}
	
	public static long getNumRows(ITask task) throws Exception {
		if(task instanceof BasicIteratorTask) {
			return ((BasicIteratorTask) task).getIterator().getNumRows();
		} else if(task instanceof MapLambdaReactor){
			return getNumRows(((MapLambdaReactor) task).getInnerTask());
		}
		
		return 0;
	}
	
	/**
	 * Flush the task data into an array
	 * This assumes you have table data!!!
	 * @param taskData
	 * @return
	 */
	public static List<Object> flushJobData(Object taskData) {
		if(taskData instanceof ITask) {
			return flushJobData((ITask) taskData);
		} else if(taskData instanceof Map) {
			Map dataMap = (Map) ((Map) taskData).get("data");
			if(dataMap != null) {
				List<Object[]> values = (List<Object[]>) dataMap.get("values");
				if(values != null) {
					List<Object> retVal = new Vector<Object>();
					int size = values.size();
					for(int i = 0; i < size; i++) {
						retVal.add(values.get(i)[0]);
					}
					return retVal;
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Flush the task data into an array
	 * This assumes you have table data!!!
	 * @param taskData
	 * @return
	 */
	private static List<Object> flushJobData(ITask taskData) {
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
		if(taskData instanceof ITask) {
			Map<String, Object> collect;
			try {
				collect = ((ITask) taskData).collect(false);
				return TaskUtility.getTaskDataScalarElement(collect);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(e.getMessage());
			}
		}
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
	
	/**
	 * Get the data types for the task based on index of header output
	 * @param task
	 * @return
	 */
	public static SemossDataType[] getTypesFromTask(ITask task) {
		List<Map<String, Object>> headerInfo = task.getHeaderInfo();
		int size = headerInfo.size();
		
		SemossDataType[] typesArr = new SemossDataType[size];
		for(int i = 0; i < size; i++) {
			Map<String, Object> hMap = headerInfo.get(i);
			typesArr[i] = SemossDataType.convertStringToDataType((String) hMap.get("type"));
		}
		return typesArr;
	}
	
	/**
	 * Get the column to data type map from a task
	 * @param task
	 * @return
	 */
	public static Map<String, SemossDataType> getTypesMapFromTask(ITask task) {
		List<Map<String, Object>> headersInfo = task.getHeaderInfo();
		Map<String, SemossDataType> typesMap = new HashMap<>();
		for (Map<String, Object> hMap : headersInfo) {
			String name = (String) hMap.get("alias");
			SemossDataType type = SemossDataType.convertStringToDataType(hMap.get("type").toString());
			typesMap.put(name, type);
		}
		return typesMap;
	}
	
}
