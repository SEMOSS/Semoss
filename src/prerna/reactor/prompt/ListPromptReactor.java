package prerna.reactor.prompt;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityPromptUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
public class ListPromptReactor extends AbstractReactor {
	
	public ListPromptReactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String userId = this.insight.getUserId();
		if (userId == null || userId.isEmpty()) {
			throw new IllegalArgumentException("User is not properly logged in.");
		}
		GenRowFilters filters = getFilters();
		String limit = this.keyValue.get(this.keysToGet[0]);
		String offset = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> promptMetadataFilter = getMetaMap();
		List<Map<String, Object>> response = SecurityPromptUtils.getPrompts(userId, filters, promptMetadataFilter, limit, offset);
		
		NounMetadata nm = new NounMetadata(response, PixelDataType.MAP);
		return nm;
	}

	protected GenRowFilters getFilters() {
		// generate a grf with the wanted filters
		GenRowFilters grf = new GenRowFilters();
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			SelectQueryStruct qs = (SelectQueryStruct) this.curRow.get(i);
			if (qs != null) {
				grf.merge(qs.getCombinedFilters());
			}
		}
		if(grf != null && !grf.isEmpty()) {
			return grf;
		}
		
		return null;
	}
	
	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.META_FILTERS.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
}
