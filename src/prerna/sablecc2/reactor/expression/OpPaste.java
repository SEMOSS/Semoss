package prerna.sablecc2.reactor.expression;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;

public class OpPaste extends OpBasic {

	public OpPaste() {
		this.operation="paste";
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey(), "sep"};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		String sep = getSep();
		StringBuilder builder = new StringBuilder();
		if (values.length != 0) {
			if(values[0] instanceof Object[]) {
				Object[] arr = (Object[]) values[0];
				if(arr.length > 0) {
					builder.append(arr[0].toString());
					for(int i = 1; i < arr.length; i++) {
						builder.append(sep).append(arr[i].toString());
					}
				} else {
					// if empty, still need to append
					builder.append(sep);
				}
			} else {
				if(values[0] instanceof ITask) {
					List<Object> taskData = TaskUtility.flushJobData((ITask) values[0]);
					for(int j = 0; j < taskData.size(); j++) {
						if(j == 0) {
							builder.append(taskData.get(j).toString());
						} else {
							builder.append(sep).append(taskData.get(j).toString());
						}
					}
				} else {
					builder.append(values[0].toString());
				}
			}
			for(int i = 1; i < values.length; i++) {
				if(values[i] instanceof Object[]) {
					Object[] arr = (Object[]) values[0];
					if(arr.length > 0) {
						builder.append(arr[0].toString());
						for(int j = 1; j < arr.length; j++) {
							builder.append(sep).append(arr[j].toString());
						}
					} else {
						// if empty, still need to append
						builder.append(sep);
					}
				} else {
					if(values[i] instanceof ITask) {
						List<Object> taskData = TaskUtility.flushJobData((ITask) values[i] );
						for(int j = 0; j < taskData.size(); j++) {
							builder.append(sep).append(taskData.get(j).toString());
						}
					} else {
						builder.append(sep).append(values[i].toString());
					}
				}
			}
		}
		NounMetadata noun = new NounMetadata(builder.toString(), PixelDataType.CONST_STRING);
		return noun;
	}

	@Override
	public String getReturnType() {
		return "String";
	}
	
	/**
	 * Get the separator
	 * @return
	 */
	private String getSep() {
		String sep = " ";
		if(this.store.getNoun(this.keysToGet[1]) != null) {
			sep = this.store.getNoun(this.keysToGet[1]).get(0).toString();
		}
		return sep;
	}

}
