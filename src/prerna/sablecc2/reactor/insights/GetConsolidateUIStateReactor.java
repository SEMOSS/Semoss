package prerna.sablecc2.reactor.insights;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.gson.InsightPanelAdapter;
import prerna.util.gson.InsightSheetAdapter;

public class GetConsolidateUIStateReactor extends AbstractReactor {

	public static final String OUTPUT_TYPE = "output";
	public static final String MAP = "map";
	public static final String STRING = "stirng";
	
	@Override
	public NounMetadata execute() {
		Gson gson = new Gson();

		List<String> pixelSteps = new Vector<String>();
		String outputType = getOutput();

		// we will be doing this for every sheet and every panel
		for(String sheetId : this.insight.getInsightSheets().keySet()) {
			pixelSteps.add("AddSheet(" + sheetId + ");");
		}
		for(String sheetId : this.insight.getInsightSheets().keySet()) {
			// we will just serialize the insight sheet
			InsightSheet sheet = this.insight.getInsightSheet(sheetId);
			InsightSheetAdapter adapter = new InsightSheetAdapter();
			String serialization = null;
			try {
				serialization = adapter.toJson(sheet);
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Exeption occured generate the sheet state with error: " + e.getMessage());
			}
			
			// turn the serialization into a Map object
			if(MAP.equals(outputType)) {
				HashMap<String, Object> json = gson.fromJson(serialization, HashMap.class);
				pixelSteps.add("SetSheetState(" + gson.toJson(json) + ");");
			} else {
				pixelSteps.add("SetSheetState(\"" + serialization + "\");");
			}
		}
		
		// repeat for each panel
		for(String panelId : this.insight.getInsightPanels().keySet()) {
			pixelSteps.add("AddPanel(" + panelId + ");");
		}
		for(String panelId : this.insight.getInsightPanels().keySet()) {
			InsightPanel panel = this.insight.getInsightPanel(panelId);
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
				HashMap<String, Object> json = gson.fromJson(serialization, HashMap.class);
				pixelSteps.add("SetPanelState(" + gson.toJson(json) + ");");
			} else {
				pixelSteps.add("SetPanelState(\"" + serialization + "\");");
			}
		}
		
		return new NounMetadata(pixelSteps, PixelDataType.VECTOR);
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
