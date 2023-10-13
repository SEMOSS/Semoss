package prerna.reactor.export;

import java.io.IOException;
import java.util.List;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class GrabScalarElementReactor extends AbstractReactor {

	/**
	 * This class is responsible for collecting the first element from a task and returning it as a noun
	 */
	
	private static final String CLEAN_UP_KEY = "cleanUp";
	
	public GrabScalarElementReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), CLEAN_UP_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		ITask task = getTask();
		if(task == null) {
			throw new IllegalArgumentException("Could not find task to retrieve data from!");
		}
		String stringType = (String) task.getHeaderInfo().get(0).get("dataType");
		
		PixelDataType nounType = null;
		Object nounValue = null;
		if(task.hasNext()) {
			nounValue = task.next().getValues()[0];
			if(Utility.isNumericType(stringType)) {
				nounType = PixelDataType.CONST_DECIMAL;
			} else {
				nounType = PixelDataType.CONST_STRING;
			}
		} else {
			nounType = PixelDataType.NULL_VALUE;
		}
		
		boolean cleanUp = cleanUp();
		if(cleanUp) {
			try {
				task.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.insight.getTaskStore().removeTask(task.getId());
		}
		
		return new NounMetadata(nounValue, nounType);
	}

	//This gets the task to collect from
	private ITask getTask() {
		ITask task;
		
		List<Object> tasks = curRow.getValuesOfType(PixelDataType.TASK);
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(tasks == null || tasks.size() == 0) {
			task = (ITask) getNounStore().getNoun(PixelDataType.TASK.getKey()).get(0);
		} else {
			task = (ITask) curRow.getValuesOfType(PixelDataType.TASK).get(0);
		}
		return task;
	}
	
	private boolean cleanUp() {
		GenRowStruct cleanUpGrs = this.store.getNoun(CLEAN_UP_KEY);
		if(cleanUpGrs != null && !cleanUpGrs.isEmpty()) {
			boolean cleanUp = (boolean) cleanUpGrs.get(0);
			return cleanUp;
		}
		
		// default is to stop the iterator and clean up
		return true;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CLEAN_UP_KEY)) {
			return "Boolean indication (true or false) to clear the task - defaults to true";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
