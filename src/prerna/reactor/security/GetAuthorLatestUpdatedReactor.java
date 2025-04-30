package prerna.reactor.security;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;


public class GetAuthorLatestUpdatedReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetAuthorLatestUpdatedReactor.class);

    public GetAuthorLatestUpdatedReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        String engineId = this.keyValue.get(this.keysToGet[0]);
        
        if(engineId == null || engineId.isEmpty()) {
            classLogger.error(Constants.STACKTRACE, new IllegalArgumentException("Must input an engine id"));
        }

        SelectQueryStruct qs = new SelectQueryStruct();
        qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSIONGRANTEDBY"));
        qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__DATEADDED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
        qs.addOrderBy(new QueryColumnOrderBySelector("ENGINEPERMISSION__DATEADDED","DESC"));
        qs.setLimit(1);

        RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

        List<Map<String, Object>> list = (List<Map<String, Object>>) QueryExecutionUtility.flushRsToMap(securityDb, qs);
        
        if(list.size() > 0){
            return new NounMetadata(list.get(0), PixelDataType.CUSTOM_DATA_STRUCTURE);
        } else {
            Map<String, Object> map = new java.util.HashMap<String, Object>();
            map.put("PERMISSIONGRANTEDBY", "No data found for engine id: " + engineId);
            map.put("DATEADDED", "No data found for engine id: " + engineId);
            list.add(map);
            return new NounMetadata(list.get(0), PixelDataType.CUSTOM_DATA_STRUCTURE);    
        }
    }
}
