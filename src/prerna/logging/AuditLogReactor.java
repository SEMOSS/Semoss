package prerna.logging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.Settings;
import prerna.util.Utility;

public class AuditLogReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(AuditLogReactor.class);

	private String loggerMicroserviceUrl = null;

	public AuditLogReactor() {
		/*
		 * this.keysToGet = new String[]{ ReactorKeysEnum.AGENT.getKey(),
		 * ReactorKeysEnum.ROOM.getKey(), ReactorKeysEnum.DATE_TIME_FIELD.getKey() };
		 */
		this.keysToGet = new String[] {ReactorKeysEnum.AUDIT_APIS_END_POINT.getKey(),ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
        this.keyRequired = new int[] {1};
        this.loggerMicroserviceUrl = Utility.getDIHelperProperty(Settings.LOGGER_MICROSERVICE_URL);
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        
        String endPoint = this.keyValue.get(ReactorKeysEnum.AUDIT_APIS_END_POINT.getKey());
       
        String url = this.loggerMicroserviceUrl +"/"+ endPoint;
		String response = HttpHelperUtility.makeGetCall(url, null, getMap(), false);
		return new NounMetadata(response, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.LOGGING_DATA);
    }
    
    /**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
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
