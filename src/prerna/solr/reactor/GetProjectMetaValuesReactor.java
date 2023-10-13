package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetProjectMetaValuesReactor extends AbstractReactor {
    
    public GetProjectMetaValuesReactor() {
        this.keysToGet = new String[] {ReactorKeysEnum.META_KEYS.getKey()};
    }
    
    @Override
    public NounMetadata execute() {
        List<String> projectIdList = SecurityProjectUtils.getUserProjectIdList(this.insight.getUser(), true, false, true);
        if(projectIdList != null && projectIdList.isEmpty()) {
        	return new NounMetadata(new ArrayList<>(), PixelDataType.CUSTOM_DATA_STRUCTURE);
        }
        List<Map<String, Object>> ret = SecurityProjectUtils.getAvailableMetaValues(projectIdList, getMetaKeys());
        return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
    }
    
    private List<String> getMetaKeys() {
        GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
        if(grs != null && !grs.isEmpty()) {
            return grs.getAllStrValues();
        }
        return this.curRow.getAllStrValues();
    }
    
}