package prerna.util.git.reactors;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class GitBaseReactor extends AbstractReactor {

	public String getToken() {
		User user = insight.getUser();
		String gitProvider = DIHelper.getInstance().getProperty(Constants.GIT_PROVIDER);
		AccessToken gitAccess = null;
		if(gitProvider != null && !(gitProvider.isEmpty()) && gitProvider.toLowerCase().equals(AuthProvider.GITLAB.toString().toLowerCase())) {
			 gitAccess = user.getAccessToken(AuthProvider.GITLAB);
		} else {
			 gitAccess = user.getAccessToken(AuthProvider.GITHUB);
		}

		if(gitAccess == null) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "git");
			retMap.put("message", "Please login to your Git account");
			throwLoginError(retMap);
		}

		return gitAccess.getAccess_token();
	}
}
