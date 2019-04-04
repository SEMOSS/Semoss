package prerna.sablecc2.reactor.frame.r.util;

import prerna.auth.User;
import prerna.engine.impl.r.RUserRserve;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class REnableUserRecoveryReactor extends AbstractReactor {

	public REnableUserRecoveryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENABLE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		User user = this.insight.getUser();
		if (user == null) throw new IllegalArgumentException("User is not defined.");
		RUserRserve rcon = user.getRcon();
		if (rcon == null) throw new IllegalArgumentException("The user's R connection is not defined.");
		
		String enableString = this.keyValue.get(this.keysToGet[0]);
		if (enableString != null) {
			rcon.setRecoveryEnabled(Boolean.parseBoolean(enableString));
		}
		
		return new NounMetadata(rcon.isRecoveryEnabled(), PixelDataType.BOOLEAN);
	}

}
