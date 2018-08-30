package prerna.sablecc2.om.nounmeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RemoveHeaderNounMetadata extends NounMetadata {

	public RemoveHeaderNounMetadata(String... headers) {
		this.value = new HashMap<String, String[]>();
		((Map) this.value).put("remove", headers);
		setConfig();
	}
	
	public RemoveHeaderNounMetadata(List<String> headers) {
		this.value = new HashMap<String, List<String>>();
		((Map) this.value).put("remove", headers);
		setConfig();
	}
	
	private void setConfig() {
		this.noun = PixelDataType.CONST_STRING;
		this.opType.add(PixelOperationType.REMOVE_HEADERS);
	}
	
}
