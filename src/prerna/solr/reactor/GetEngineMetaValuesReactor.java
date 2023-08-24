package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetEngineMetaValuesReactor extends AbstractReactor {
    
    public GetEngineMetaValuesReactor() {
        this.keysToGet = new String[] {ReactorKeysEnum.ENGINE_TYPE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
    }
    
    @Override
    public NounMetadata execute() {
    	List<String> eTypes = getListValues(ReactorKeysEnum.ENGINE_TYPE.getKey());
        List<String> engineList = SecurityEngineUtils.getUserEngineIdList(this.insight.getUser(), eTypes, true, false, true);
        if(engineList != null && engineList.isEmpty()) {
        	return new NounMetadata(new ArrayList<>(), PixelDataType.CUSTOM_DATA_STRUCTURE);
        }
        List<Map<String, Object>> ret = SecurityEngineUtils.getAvailableMetaValues(engineList, getListValues(ReactorKeysEnum.META_KEYS.getKey()) );
        return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
    }
    
    private List<String> getListValues(String key) {
        GenRowStruct grs = this.store.getNoun(key);
        if(grs != null && !grs.isEmpty()) {
            return grs.getAllStrValues();
        }
        return this.curRow.getAllStrValues();
    }
    
}