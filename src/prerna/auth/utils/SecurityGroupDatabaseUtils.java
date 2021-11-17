package prerna.auth.utils;

import java.util.List;

import prerna.auth.AuthProvider;
import prerna.auth.User;

public class SecurityGroupDatabaseUtils extends AbstractSecurityUtils {
	
	public static boolean userCanViewDatabase(User user, String databaseId) {
		// query if user groups has access to the db
		List<AuthProvider> logins = user.getLogins();
		// query will filter on AccessPermission
		
		
		return false;
	}
	
}
