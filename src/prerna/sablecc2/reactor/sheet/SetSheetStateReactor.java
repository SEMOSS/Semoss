package prerna.sablecc2.reactor.sheet;

import java.io.IOException;
import java.util.List;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;
import prerna.util.gson.InsightSheetAdapter;

public class SetSheetStateReactor extends AbstractInsightPanelReactor {

	/*
	 * This class is complimentary to GetSheetStateReactor
	 */
	
	private static final String STATE = "state";
	
	public SetSheetStateReactor() {
		this.keysToGet = new String[] {STATE};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String serialized = getSerialization();
		if(serialized == null) {
			throw new NullPointerException("Serialization of the sheet state is null");
		}
		// we will just read the serialization of the insight panel
		InsightSheetAdapter adapter = new InsightSheetAdapter();
		InsightSheet insightSheet = null;
		try {
			insightSheet = adapter.fromJson(serialized);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Exeption occured reading the panel state with error: " + e.getMessage());
		}
		
		this.insight.getInsightSheets().put(insightSheet.getSheetId(), insightSheet);
		NounMetadata noun = new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.CACHED_SHEET);
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
				return strInput.get(0);
			}
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				Gson gson = new Gson();
				return gson.toJson(mapInput.get(0));
			}
		}
		
		if(!this.curRow.isEmpty()) {
			List<String> strInput = this.curRow.getAllStrValues();
			if(strInput != null && !strInput.isEmpty()) {
				return strInput.get(0);
			}
			List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				Gson gson = new Gson();
				return gson.toJson(mapInput.get(0));
			}
		}
		
		return null;
	}
	
}
