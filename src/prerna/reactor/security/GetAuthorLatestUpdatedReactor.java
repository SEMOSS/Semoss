package prerna.reactor.security;

import java.util.Date;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;


public class GetAuthorLatestUpdatedReactor extends AbstractReactor {

    public GetAuthorLatestUpdatedReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        
        if(engineId == null || engineId.isEmpty()) {
            throw new IllegalArgumentException("Must input an engine id");
        }

        User user = this.insight.getUser();
        SelectQueryStruct qs = new SelectQueryStruct();
        qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSIONGRANTEDBY"));
        qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__DATEADDED"));
        qs.addOrderBy(new QueryColumnOrderBySelector("ENGINEPERMISSION__DATEADDED","DESC"));
        qs.setLimit(1);

        RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

        // IRawSelectWrapper wrapper = null;
        // try {
		// 	wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
        // } catch (Exception e) {
        //     throw new RuntimeException("Error getting engine metadata", e);
        // }

        List<Map<String, Object>> list = (List<Map<String, Object>>) QueryExecutionUtility.flushRsToMap(securityDb, qs);
        return NounMetadata.getSuccessNounMessage("CORs were updated Successfully!");

    }
}
