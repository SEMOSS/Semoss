package prerna.reactor.panel;

import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.date.SemossDate;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.gson.InsightPanelAdapter;
import prerna.util.gson.NumberAdapter;
import prerna.util.gson.SemossDateAdapter;

public class SetPanelStateReactor extends AbstractInsightPanelReactor {

	/*
	 * This class is complimentary to GetPanelStateReactor
	 */
	
	private static final Gson GSON =  new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeAdapter(Double.class, new NumberAdapter())
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.create();
	
	private static final String STATE = "state";
	
	public SetPanelStateReactor() {
		this.keysToGet = new String[] {STATE};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String serialized = getSerialization();
		if(serialized == null) {
			throw new NullPointerException("Serialization of the panel state is null");
		}
		// we will just read the serialization of the insight panel
		InsightPanelAdapter adapter = new InsightPanelAdapter();
		InsightPanel insightPanel = null;
		try {
			insightPanel = adapter.fromJson(serialized);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Exeption occurred reading the panel state with error: " + e.getMessage());
		}
		this.insight.getInsightPanels().put(insightPanel.getPanelId(), insightPanel);
		NounMetadata noun = new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.CACHED_PANEL);
		return noun;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(STATE)) {
			return "The serialization for the insight panel";
		}
		return super.getDescriptionForKey(key);
	}
	
	private String getSerialization() {
		GenRowStruct grs = this.store.getNoun(STATE);
		if(grs != null && !grs.isEmpty()) {
			List<String> strInput = grs.getAllStrValues();
			if(strInput != null && !strInput.isEmpty()) {
				return Utility.decodeURIComponent(strInput.get(0));
			}
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				return GSON.toJson(mapInput.get(0));
			}
		}
		
		if(!this.curRow.isEmpty()) {
			List<String> strInput = this.curRow.getAllStrValues();
			if(strInput != null && !strInput.isEmpty()) {
				return Utility.decodeURIComponent(strInput.get(0));
			}
			List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				return GSON.toJson(mapInput.get(0));
			}
		}
		
		return null;
	}
	
}
