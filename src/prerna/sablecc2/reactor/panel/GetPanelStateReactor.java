package prerna.sablecc2.reactor.panel;

import java.io.IOException;
import java.util.HashMap;

import com.google.gson.Gson;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.InsightPanelAdapter;

public class GetPanelStateReactor extends AbstractInsightPanelReactor {

	/*
	 * This class is complimentary to SetPanelStateReactor
	 */
	
	public static final String OUTPUT_TYPE = "output";
	public static final String MAP = "map";
	public static final String STRING = "stirng";
	
	public GetPanelStateReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), OUTPUT_TYPE};
	}
	
	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new NullPointerException("No panel was passed in to get the state");
		}
		String outputType = getOutput();
		
		// we will just serialize the insight panel
		InsightPanelAdapter adapter = new InsightPanelAdapter();
		String serialization = null;
		try {
			serialization = adapter.toJson(panel);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Exeption occured generate the panel state with error: " + e.getMessage());
		}
		
		// turn the serialization into a Map object
		if(MAP.equals(outputType)) {
			HashMap<String, Object> json = new Gson().fromJson(serialization, HashMap.class);
			return new NounMetadata(json, PixelDataType.MAP);
		}
		return new NounMetadata(serialization, PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(OUTPUT_TYPE)) {
			return "The value to return - as a 'string' or 'map'";
		}
		return super.getDescriptionForKey(key);
	}
	
	private String getOutput() {
		GenRowStruct grs = this.store.getNoun(OUTPUT_TYPE);
		if(grs != null && !grs.isEmpty()) {
			String input = grs.get(0).toString();
			if(input.equalsIgnoreCase(STRING)) {
				return STRING;
			} else {
				return MAP;
			}
		}
		
		if(!this.curRow.isEmpty()) {
			if(this.curRow.toString().equalsIgnoreCase(STRING)) {
				return STRING;
			} else {
				return MAP;
			}
		}
		
		return MAP;
	}
	
}
