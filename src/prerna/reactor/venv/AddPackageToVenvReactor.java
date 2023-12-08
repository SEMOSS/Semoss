package prerna.reactor.venv;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVenvEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddPackageToVenvReactor extends AbstractReactor {

	public AddPackageToVenvReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}
		
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if(!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Virtual Environment " + engineId + " does not exist or user does not have access to it");
		}
		
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			throw new SemossPixelException("Unable to get param values map.");
		}
		
		IVenvEngine engine = Utility.getVenvEngine(engineId);
		if (engine == null) {
			throw new SemossPixelException("Unable to find engine");
		}

		try {
			engine.addPackage(paramMap);
		} catch (Exception e) {
			throw new SemossPixelException("Unable to run process to add package to virtual environment: " + e.getMessage());
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
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