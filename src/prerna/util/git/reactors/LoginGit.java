package prerna.util.git.reactors;

import org.apache.log4j.Logger;
import org.kohsuke.github.GitHub;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitUtils;

public class LoginGit extends AbstractReactor {

	public LoginGit() {
		this.keysToGet = new String[]{ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Logging In...");
		String username = this.keyValue.get(this.keysToGet[0]);
		String password = this.keyValue.get(this.keysToGet[1]);
		GitHub ret = GitUtils.login(username, password);
		if(ret == null) {
			throw new IllegalArgumentException("Could not properly login using credentials");
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}
}
