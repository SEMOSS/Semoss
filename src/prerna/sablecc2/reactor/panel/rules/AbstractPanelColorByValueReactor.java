package prerna.sablecc2.reactor.panel.rules;

import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

public abstract class AbstractPanelColorByValueReactor extends AbstractInsightPanelReactor {

	protected static final String LEGACY_KEY = "panelCbv";
	
	/**
	 * Get the color by value id from the input
	 */
	protected String getCbvId(int keyToGetIndex) {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[keyToGetIndex]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		// try if it is a string
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(strNouns != null && !strNouns.isEmpty()) {
			return strNouns.get(0).getValue().toString();
		}
		
		// TRY THE LEGACY KEY!
		GenRowStruct legacyReactorGrs = this.store.getNoun(LEGACY_KEY);
		if(legacyReactorGrs != null && !legacyReactorGrs.isEmpty()) {
			return legacyReactorGrs.get(0).toString();
		}
		
		// well, you are out of luck
		throw new IllegalArgumentException("Need to specify the color by value id");
	}
}
