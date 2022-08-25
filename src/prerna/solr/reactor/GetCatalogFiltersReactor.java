package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetCatalogFiltersReactor extends AbstractReactor {
    
    public GetCatalogFiltersReactor() {
        this.keysToGet = new String[] {ReactorKeysEnum.META_KEYS.getKey()};
    }
    
    @Override
    public NounMetadata execute() {
        List<String> keys = getMetaKeys();
        List<String> dbList = null;
        if(AbstractSecurityUtils.securityEnabled()) {
            dbList = SecurityDatabaseUtils.getUserDatabaseIdList(this.insight.getUser());
        } else {
        	dbList = SecurityDatabaseUtils.getAllDatabaseIds();
        }
        if(dbList != null && dbList.isEmpty()) {
            return NounMetadata.getErrorNounMessage("You do not have access to any databases");
        }
        List<Map<String, Object>> ret = SecurityDatabaseUtils.getAvailableMetaValues(dbList, keys);
        NounMetadata noun = new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
        
        return noun;
    }
    
    private List<String> getMetaKeys() {
        GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
        if(grs != null && !grs.isEmpty()) {
            return grs.getAllStrValues();
        }
        return this.curRow.getAllStrValues();
    }
    
}