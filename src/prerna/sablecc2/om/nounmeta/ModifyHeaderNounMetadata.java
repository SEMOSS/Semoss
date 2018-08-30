package prerna.sablecc2.om.nounmeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ModifyHeaderNounMetadata extends NounMetadata {

	public ModifyHeaderNounMetadata(String origHeader, String newHeader) {
		this.value = new HashMap<String, String[]>();
		((Map) this.value).put("remove", new String[]{origHeader});
		((Map) this.value).put("add", new String[]{newHeader});

		setConfig();
	}
	
	public ModifyHeaderNounMetadata(List<String> origHeaders, List<String> newHeaders) {
		this.value = new HashMap<String, List<String>>();
		((Map) this.value).put("remove", origHeaders);
		((Map) this.value).put("add", newHeaders);

		setConfig();
	}
	
	public ModifyHeaderNounMetadata(String[] origHeaders, String[] newHeaders) {
		this.value = new HashMap<String, String[]>();
		((Map) this.value).put("remove", origHeaders);
		((Map) this.value).put("add", newHeaders);

		setConfig();
	}
	
	private void setConfig() {
		this.noun = PixelDataType.CONST_STRING;
		this.opType.add(PixelOperationType.MODIFY_HEADERS);
	}
	
}
