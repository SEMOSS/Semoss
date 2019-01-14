package prerna.sablecc2.reactor.export;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.AbstractReactor;

public class AsTaskReactor extends AbstractReactor {

	/**
	 * This class is responsible for collecting the first element from a task and returning it as a noun
	 */
	
	public AsTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		NounMetadata inputValue = getInputValue();
		List<Object[]> dataValues = new Vector<Object[]>();
		dataValues.add(new Object[]{inputValue.getValue()});
		
		ConstantDataTask task = new ConstantDataTask();
		Map<String, Object> returnData = new Hashtable<String, Object>();
		returnData.put("values", dataValues);
		returnData.put("headers", new String[]{"constant"});
		task.setOutputData(returnData);
		
		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}
	
	private NounMetadata getInputValue() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.getNoun(0);
		}
		
		Set<String> inKeys = new HashSet<String>(this.store.getNounKeys());
		inKeys.remove("all");
		inKeys.remove(this.keysToGet[0]);
		for(String k : inKeys) {
			grs = this.store.getNoun(k);
			if(grs != null && !grs.isEmpty()) {
				return grs.getNoun(0);
			}
		}
		
		if(this.curRow != null && !this.curRow.isEmpty()) {
			return this.curRow.getNoun(0);
		}
		
		return null;
	}

}
