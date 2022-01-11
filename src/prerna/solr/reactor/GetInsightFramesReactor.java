package prerna.solr.reactor;

import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityUserProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightFramesReactor extends AbstractReactor {

	public GetInsightFramesReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);

		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityUserProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, rdbmsId)) {
				NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		
		List<Object[]> retList = SecurityInsightUtils.getInsightFrames(projectId, rdbmsId);
		NounMetadata retNoun = new NounMetadata(retList, PixelDataType.CUSTOM_DATA_STRUCTURE);
		return retNoun;
	}
}
