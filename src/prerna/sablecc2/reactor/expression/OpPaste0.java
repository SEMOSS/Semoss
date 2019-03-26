package prerna.sablecc2.reactor.expression;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;

public class OpPaste0 extends OpBasic {

	public OpPaste0() {
		this.operation="paste";
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey(), "sep"};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		StringBuilder builder = new StringBuilder();
		if (values.length != 0) {
			if(values[0] instanceof Object[]) {
				Object[] arr = (Object[]) values[0];
				if(arr.length > 0) {
					builder.append(arr[0].toString());
					for(int i = 1; i < arr.length; i++) {
						builder.append(arr[i].toString());
					}
				}
			} else {
				if(values[0] instanceof ITask) {
					List<Object> taskData = TaskUtility.flushJobData((ITask) values[0]);
					for(int j = 0; j < taskData.size(); j++) {
						builder.append(taskData.get(j).toString());
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
							builder.append(arr[j].toString());
						}
					}
				} else {
					if(values[i] instanceof ITask) {
						List<Object> taskData = TaskUtility.flushJobData((ITask) values[i] );
						for(int j = 0; j < taskData.size(); j++) {
							builder.append(taskData.get(j).toString());
						}
					} else {
						builder.append(values[i].toString());
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
}
