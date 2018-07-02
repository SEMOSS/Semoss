package prerna.sablecc2.reactor.workflow;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ModifyInsightDatasourceReactor extends AbstractReactor {

	public ModifyInsightDatasourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.OPTIONS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> replacementOptions = getOptions();

		List<String> recipe = this.insight.getPixelRecipe();
		StringBuilder b = new StringBuilder();
		for(String s : recipe) {
			b.append(s);
		}
		String fullRecipe = b.toString();
		
		List<String> newRecipe = PixelUtility.modifyInsightDatasource(this.insight.getUser(), fullRecipe, replacementOptions);
		
		//TODO: added this in for testing!!!
		this.insight.getPixelRecipe().clear();
		return new NounMetadata(newRecipe, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	/**
	 * Get the replacement information
	 * @return
	 */
	public List<Map<String, Object>> getOptions() {
		List<Map<String, Object>> ret = new Vector<Map<String, Object>>();
		
		GenRowStruct options = this.store.getNoun(this.keysToGet[0]);
		if(options != null && !options.isEmpty()) {
			int size = options.size();
			for(int i = 0; i < size; i++) {
				ret.add( (Map<String, Object>) options.get(i));
			}
			return ret;
		}
		
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			ret.add( (Map<String, Object>) this.curRow.get(i));
		}
		return ret;
	}

}
