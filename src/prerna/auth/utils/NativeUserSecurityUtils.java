package prerna.auth.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.auth.AccessToken;

public class NativeUserSecurityUtils extends AbstractSecurityUtils {

	private NativeUserSecurityUtils() {
		
	}
	
	/*
	 * Native user CRUD 
	 */
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public static Boolean addNativeUser(AccessToken newUser, String password) throws IllegalArgumentException{
		validInformation(newUser, password);
		boolean isNewUser = SecurityQueryUtils.checkUserExist(newUser.getUsername(), newUser.getEmail());
		if(!isNewUser) {			
			String salt = SecurityQueryUtils.generateSalt();
			String hashedPassword = (SecurityQueryUtils.hash(password, salt));
			String query = "INSERT INTO USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT) VALUES ('" + newUser.getEmail() + "', '"+ newUser.getName() + "', '" + newUser.getUsername() + "', '" + newUser.getEmail() + "', '" + newUser.getProvider() + "', 'FALSE', "
					+ "'" + hashedPassword + "', '" + salt + "');";
			
			securityDb.insertData(query);
			securityDb.commit();
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Basic validation of the user information before creating it.
	 * @param newUser
	 * @throws IllegalArgumentException
	 */
	static void validInformation(AccessToken newUser, String password) throws IllegalArgumentException {
		String error = "";
		if(newUser.getUsername().isEmpty()){
			error += "User name can not be empty. ";
		}

		error += validEmail(newUser.getEmail());
		error += validPassword(password);
		
		if(!error.isEmpty()){
			throw new IllegalArgumentException(error);
		}
	}
	
	static String validEmail(String email){
		if(!email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]{2,}$")){
			return  email + " is not a valid email address. ";
		}
		return "";
	}
	
	static String validPassword(String password){
		Pattern pattern = Pattern.compile("^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#$%^&*])(?=.{8,})");
        Matcher matcher = pattern.matcher(password);
		
		if(!matcher.lookingAt()){
			return "Password doesn't comply with the security policies.";
		}
		return "";
	}
}
