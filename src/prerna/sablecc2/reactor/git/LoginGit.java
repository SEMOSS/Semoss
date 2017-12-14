package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class LoginGit extends AbstractReactor {

	public LoginGit() {
		this.keysToGet = new String[]{"username", "password"};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		organizeKeys();
		logger.info("Logging In...");
		boolean valid = new GitHelper().login(this.keyValue.get(this.keysToGet[0]), this.keyValue.get(this.keysToGet[1]));
		if(valid) {
			logger.info("Successfully logged in");
		} else {
			throw new IllegalArgumentException("Invalid Git credentials");
		}
		return new NounMetadata(valid, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}
}
