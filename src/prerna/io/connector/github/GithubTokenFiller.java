package prerna.io.connector.github;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GHMyself;
import org.kohsuke.github.GitHub;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.util.Constants;

public class GithubTokenFiller implements IAccessTokenFiller {

	private static final Logger classLogger = LogManager.getLogger(GithubTokenFiller.class);

	@Override
	public void fillAccessToken(AccessToken gitAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params) {
		// add specific Git values
		GHMyself myGit = null;
		try {
			myGit = GitHub.connectUsingOAuth(gitAccessToken.getAccess_token()).getMyself();
			gitAccessToken.setId(myGit.getId() + "");
			gitAccessToken.setEmail(myGit.getEmail());
			gitAccessToken.setName(myGit.getName());
			gitAccessToken.setLocale(myGit.getLocation());
			gitAccessToken.setUsername(myGit.getLogin());
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
	
}