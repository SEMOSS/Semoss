package prerna.engine.impl.model.workspace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DeleteWorkspaceDocumentReactor extends AbstractReactor {
	
	private static final Logger LOGGER = LogManager.getLogger(AddWorkspaceDocumentReactor.class);
	
	public static final String WORKSPACE_ID = "workspaceId";
	public static final String FILE_NAMES = "fileNames";
	
	public DeleteWorkspaceDocumentReactor() {
		this.keysToGet = new String[] { WORKSPACE_ID, FILE_NAMES, ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		User user = this.insight.getUser();
		
		String workspaceId = this.keyValue.get(WORKSPACE_ID);
		List<String> fileNames = getFiles();
		if(fileNames.isEmpty()) {
			throw new IllegalArgumentException("fileNames to delete is empty");
		}
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		// validate workspace and permission
		Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if(current == null) {
			throw new IllegalArgumentException("Workspace not found");
		}
		String currentOwner = (String) current.get("owner");
		Boolean currentlyShared = (Boolean) current.get("sharing_enabled");
		if(currentlyShared == null) currentlyShared = Boolean.FALSE;
		boolean hasOwnerPermission = false;
		if(currentOwner != null) {
			for(AuthProvider provider : user.getLogins()) {
				if(currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
					hasOwnerPermission = true;
					break;
				}
			}
		}
		int permissionLevel = Math.min(hasOwnerPermission ? AccessPermissionEnum.OWNER.getId() : Integer.MAX_VALUE, currentlyShared ? ModelInferenceLogsUtils.getWorkspaceSharePermission(workspaceId, user, AccessPermissionEnum.OWNER.getId(), AccessPermissionEnum.EDIT.getId()) : Integer.MAX_VALUE);
		if(permissionLevel > AccessPermissionEnum.EDIT.getId()) {
			throw new IllegalArgumentException("User unauthorized to perform this operation");
		}
		
		// do the actual delete
		IVectorDatabaseEngine eng = Utility.getVectorDatabase(workspaceId);
		try {
			eng.removeDocument(fileNames, paramMap);
		} catch (Exception e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred attempting to delete the files. Detailed message = "+e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}
	
	private List<String> getFiles() {
		List<String> filePaths = new ArrayList<>();

		GenRowStruct grs = this.store.getNoun(FILE_NAMES);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				filePaths.add(grs.get(i).toString());
			}
			return filePaths;
		}

		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			filePaths.add(this.curRow.get(i).toString());
		}
		return filePaths;
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
