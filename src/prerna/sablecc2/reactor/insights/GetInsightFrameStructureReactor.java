package prerna.sablecc2.reactor.insights;

import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightFrameStructureReactor extends AbstractReactor {
	
	/*
	 * PAYLOAD MUST MATCH THAT OF 
	 * {@link prerna.sablecc2.reactor.frame.GetFrameTableStructureReactor}
	 */
	
	public GetInsightFrameStructureReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);		

		User user = this.insight.getUser();
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
			if(!SecurityInsightUtils.userCanViewInsight(user, projectId, rdbmsId)) {
				NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} else {
			projectId = MasterDatabaseUtility.testDatabaseIdIfAlias(projectId);
		}
		
		// get a list of frames info 
		List<Object[]> insightFrames = SecurityInsightUtils.getInsightFrames(projectId, rdbmsId);
		
		return new NounMetadata(insightFrames, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}