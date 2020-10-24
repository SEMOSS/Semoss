package prerna.sablecc2.reactor.app;

import java.sql.SQLException;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class RdbmsReconnectReactor extends AbstractReactor {

	public RdbmsReconnectReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		// make sure user has at least edit access
		if (AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
				if (!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
					throw new IllegalArgumentException("User does not have permission to re-establish the connection for this app");
				}
			}
		}
		
		IEngine engine = Utility.getEngine(appId);
		if(!(engine instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be an RDBMS native engine");
		}
		
		RDBMSNativeEngine rdbms = (RDBMSNativeEngine) engine;
		try {
			rdbms.makeConnection().close();
			rdbms.makeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
