package prerna.reactor.frame.r.util;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.r.IRUserConnection;
import prerna.engine.impl.r.RserveConnectionPool;
import prerna.engine.impl.r.RserveUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RClearAllUserRservesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throw new IllegalArgumentException("User must be an admin to perform this action.");
		}
		
		boolean success = true;
		if (RserveUtil.IS_USER_RSERVE) {
			if (RserveUtil.R_USER_CONNECTION_TYPE.equals(IRUserConnection.POOLED)) {
				try {
					RserveConnectionPool.getInstance().shutdown();
				} catch (Exception e) {
					success = false;
				}
			}
			try {
				RserveUtil.endR();
			} catch (Exception e) {
				success = false;
			}
		}
		
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}

}
