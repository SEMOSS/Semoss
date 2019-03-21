package prerna.sablecc2.reactor.cluster;

import java.io.IOException;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PullUserSpaceReactor extends AbstractReactor{

	@Override
	public NounMetadata execute() {
		if(ClusterUtil.IS_CLUSTER){
			User user = this.insight.getUser();
			AuthProvider token = user.getLogins().get(0);
			String userSpaceId = token.toString() + "_" + user.getAccessToken(token).getId();

			CloudClient.getClient();
			try {
				// pull the user space based on the cloud client for storage
				CloudClient.getClient().pullUser(userSpaceId);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Error occurred pulling user space");
			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.USER_UPLOAD);
	}

}
