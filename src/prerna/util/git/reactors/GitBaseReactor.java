package prerna.util.git.reactors;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class GitBaseReactor extends AbstractReactor {


	public String getToken() {

		
		User2 user = insight.getUser2();
		String oauth = null;
		AccessToken gitAccess = user.getAccessToken(AuthProvider.GIT);
		
		if(gitAccess == null)
		{
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "git");
			retMap.put("message", "Please login to your Git account");
			throwLoginError(retMap);
		}

		return gitAccess.getAccess_token();
	}
}
