package prerna.sablecc2.reactor.panel.events;

import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;
import prerna.util.Utility;

public class AddPanelEventsReactor extends AbstractInsightPanelReactor {
	
	public AddPanelEventsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.EVENTS_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the events that come as a map
		Map<String, Object> events = getEventsMapInput();
		if(events == null) {
			throw new IllegalArgumentException("Need to define the events input");
		}
		decodeQuery(events);
		// merge the map options
		insightPanel.addEvents(events);
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_EVENT);
	}

	private Map<String, Object> getEventsMapInput() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (panelNouns != null && !panelNouns.isEmpty()) {
			return (Map<String, Object>) panelNouns.get(0).getValue();
		}

		// well, you are out of luck
		return null;
	}

	/**
	 * Method to decode the query portion of the events map This is needed
	 * because our grammar does not allow for look behind logic for escaped
	 * quotes when deciding what a string is/is not So we encode it, but we need
	 * to decode it so that it is an accurate pixel
	 * 
	 * @param events
	 */
	private void decodeQuery(Map<String, Object> events) {
		// need to find the query
		// FE will always send {someKey1 : {someKey2 : [ {query : STRING TO
		// DECODE} ] } }
		// however, there may be multiple of them... so need to look through all
		// just in case
		ALL: for (String someKey1 : events.keySet()) {
			Object innerObj1 = events.get(someKey1);
			if (innerObj1 instanceof Map) {
				Map<String, Object> innerMap1 = (Map<String, Object>) innerObj1;
				for (String someKey2 : innerMap1.keySet()) {
					Object innerObj2 = innerMap1.get(someKey2);
					if (innerObj2 instanceof List) {
						List array = (List) innerObj2;
						int size = array.size();
						for (int i = 0; i < size; i++) {
							Object arrValue = array.get(i);
							if (arrValue instanceof Map) {
								Map<String, Object> mapICareAbout = (Map<String, Object>) arrValue;
								if (mapICareAbout.containsKey("query")) {
									String query = mapICareAbout.get("query").toString();
									String newQuery = Utility.decodeURIComponent(query);
									mapICareAbout.put("query", newQuery);
								}
							}
						}
						continue ALL;
					}
				}
			}
		}
	}
}
