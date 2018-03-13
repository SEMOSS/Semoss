package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddEditRuleReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		List<Object> ruleList = this.curRow.getValuesOfType(PixelDataType.MAP);
			Map<String, Object> mapOptions = (Map<String, Object>) ruleList.get(0);
			String name = (String) mapOptions.get("name");
			String description = (String) mapOptions.get("description");
			String rule = (String) mapOptions.get("rule");
			// validate rule
			// create vector 
			
		
		return null;
	}

}
