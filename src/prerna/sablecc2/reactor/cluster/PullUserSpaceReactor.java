package prerna.sablecc2.reactor.cluster;

import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PullUserSpaceReactor extends AbstractReactor{

	public PullUserSpaceReactor() {

	}

	@Override
	public NounMetadata execute() {
		if(!ClusterUtil.IS_CLUSTER){
			String userID = this.insight.getUserId();
			if(userID == null || userID.isEmpty()) {
				throw new IllegalArgumentException("Must have a user id to pull user space");
			}

			CloudClient.getClient();
			try {
				// pull the user space based on the cloud client for storage
				CloudClient.getClient().pullUser(userID);
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Error occurred pulling user space");
			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.USER_UPLOAD);
	}

}
