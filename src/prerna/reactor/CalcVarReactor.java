package prerna.reactor;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class CalcVarReactor extends AbstractReactor {

	public String[] keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	// which of these are optional : 1 means required, 0 means optional
	public int[] keyRequired = new int[] {0}; // if nothing is given calculate everything

	// sample - String [] formulas = new String[]{"x=1", "age_sum = frame_d['age'].astype(int).sum()", "msg = 'Total Age now is {}'.format(age_sum)"};	
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// in case variable or variableName is passed
		List dynamicVarNames = null;
		if(this.getNounStore().getNoun(keysToGet[0]) != null) {
			dynamicVarNames = this.getNounStore().getNoun(this.keysToGet[0]).getAllValues();
		} else if(!this.curRow.isEmpty()) {
			dynamicVarNames = this.curRow.getAllValues();
		} else {
			dynamicVarNames = insight.getAllVars();
		}
		
		// moved the existing logic to InsightUtility to easily use as a static utility method
		Map<String, Object> varValue = InsightUtility.calculateDynamicVars(this.insight, dynamicVarNames);
		return new NounMetadata(varValue, PixelDataType.MAP);
	}

}
