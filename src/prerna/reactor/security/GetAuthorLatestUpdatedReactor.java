package prerna.reactor.security;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.User;
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

        User user = this.insight.getUser();
		
		List<Map<String, Object>> baseInfo = null;
		// make sure valid id for user
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		if(SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			// user has access!
			baseInfo = SecurityEngineUtils.getUserEngineList(user, engineId, null);
		} else {
			// you dont have access
			throw new IllegalArgumentException("Engine does not exist or user does not have access to the database");
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
            throw new NoSuchElementException("No author found for the engine id: " + engineId); 
        }
    }
}
