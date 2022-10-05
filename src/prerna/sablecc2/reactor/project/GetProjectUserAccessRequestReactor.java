package prerna.sablecc2.reactor.project;

import java.util.List;
import java.util.Map;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetProjectUserAccessRequestReactor extends AbstractReactor {
	public GetProjectUserAccessRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(projectId == null) {
			throw new IllegalArgumentException("Please define the project id.");
		}
		List<Map<String, Object>> requests;
		if(AbstractSecurityUtils.securityEnabled()) {
			requests = SecurityProjectUtils.getUserAccessRequestsByProject(projectId);
		} else {
			requests = null;
		}
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}
}
