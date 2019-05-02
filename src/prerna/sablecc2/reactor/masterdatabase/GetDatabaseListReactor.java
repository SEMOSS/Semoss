package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetDatabaseListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> retList = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			retList = SecurityQueryUtils.getUserDatabaseList(this.insight.getUser());
		} else {
			retList = SecurityQueryUtils.getAllDatabaseList();
		}
		
		return new NounMetadata(retList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
	}

}
