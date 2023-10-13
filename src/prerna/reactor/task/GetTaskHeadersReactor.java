package prerna.reactor.task;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class GetTaskHeadersReactor extends AbstractReactor {
	
	public GetTaskHeadersReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> headerInfo = null;
		Object obj = this.curRow.get(0);
		if(obj instanceof ITask) {
			headerInfo = ((ITask) obj).getHeaderInfo();
		} else  if(obj instanceof Map) {
			headerInfo = (List<Map<String, Object>>) ((Map) obj).get("headerInfo");
		}
		String[] headers = new String[headerInfo.size()];
		for(int i = 0; i < headerInfo.size(); i++) {
			headers[i] = (String) headerInfo.get(i).get("alias");
		}
		return new NounMetadata(headers, PixelDataType.CONST_STRING);
	}

}
