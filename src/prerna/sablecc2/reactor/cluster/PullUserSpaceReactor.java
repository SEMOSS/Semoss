package prerna.sablecc2.reactor.cluster;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class PullUserSpaceReactor extends AbstractReactor{

	public PullUserSpaceReactor() {
	}

	@Override
	public NounMetadata execute() {

		String userID = this.insight.getUserId();


		Map<String, Object> PullUserData = new HashMap<String, Object>();

		if(!ClusterUtil.IS_CLUSTER){
			throw new IllegalArgumentException("SEMOSS is not in clustered mode");
		}

		if(userID == null || userID.isEmpty()) {
			throw new IllegalArgumentException("Must have a user id to pull");
		}
		
		CloudClient.getClient();
		try{
				CloudClient.getClient().pullUser(userID);
				PullUserData.put("pulled", userID);
			} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




		return new NounMetadata(PullUserData, PixelDataType.MAP, PixelOperationType.USER_UPLOAD);
	}

}
