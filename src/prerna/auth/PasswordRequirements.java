package prerna.auth;

import java.util.HashMap;
import java.util.Map;

import prerna.engine.api.IDatabase;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public class PasswordRequirements {

	// minimum password length
	// at least one upper case
	// at least one lower case
	// at least one numeric
	// at least one special character
	// enable password expiration
	// 		# of days
	// password expiration requires admin reset
	// allow users to change their own password
	// prevent password reuse
	//		# of passwords to remember

	private static PasswordRequirements instance = null;

	private int minPassLength = 0;
	private boolean requireUpperCase = false;
	private boolean requireLowerCase = false;
	private boolean requireNumeric = false;
	private boolean requireSpecial = false;
	private int passwordExpirationDays = -1;
	private boolean requireAdminResetForExpiration = false;
	private boolean allowUserChangePassword = false;
	private int passReuseCount = -1;
	private int daysToLock = -1;
	private int daysToLockEmail = 14;
	
	public static PasswordRequirements getInstance() throws Exception {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(PasswordRequirements.class) {
				if(instance != null) {
					return instance;
				}
				
				instance = new PasswordRequirements();
				instance.loadRequirements();
			}
		}

		return instance;
	}

	public void loadRequirements() throws Exception {
		// pull the necessary details
		String[] colNames = new String[] { 
				"PASS_LENGTH", "REQUIRE_UPPER", "REQUIRE_LOWER", "REQUIRE_NUMERIC", "REQUIRE_SPECIAL", 
				"EXPIRATION_DAYS", "ADMIN_RESET_EXPIRATION", "ALLOW_USER_PASS_CHANGE", "PASS_REUSE_COUNT",
				"DAYS_TO_LOCK", "DAYS_TO_LOCK_WARNING"};
		
		IDatabase securityDb = Utility.getEngine(Constants.SECURITY_DB);
		SelectQueryStruct qs = new SelectQueryStruct();
		for(String c : colNames) {
			qs.addSelector(new QueryColumnSelector("PASSWORD_RULES__" + c));
		}
		
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(iterator.hasNext()) {
				Object[] data = iterator.next().getValues();
				int index = 0;
				this.minPassLength = ((Number) data[index++]).intValue();
				this.requireUpperCase = (Boolean) data[index++];
				this.requireLowerCase = (Boolean) data[index++];
				this.requireNumeric = (Boolean) data[index++];
				this.requireSpecial = (Boolean) data[index++];
				this.passwordExpirationDays = ((Number) data[index++]).intValue();
				this.requireAdminResetForExpiration = (Boolean) data[index++];
				this.allowUserChangePassword = (Boolean) data[index++];
				this.passReuseCount = ((Number) data[index++]).intValue();
				Number daysToLockNum = (Number) data[index++];
				if(daysToLockNum != null) {
					this.daysToLock = daysToLockNum.intValue();
				}
				Number daysToLockEmailNum = (Number) data[index++];
				if(daysToLockEmailNum != null) {
					this.daysToLockEmail = daysToLockEmailNum.intValue();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if(iterator != null) {
				iterator.cleanUp();
			}
		}
	}
	
	public Map<String, Object> getAllPasswordRequirements() {
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("minPassLength", this.minPassLength);
		retMap.put("requireUpperCase", this.requireUpperCase);
		retMap.put("requireLowerCase", this.requireLowerCase);
		retMap.put("requireNumeric", this.requireNumeric);
		retMap.put("requireSpecial", this.requireSpecial);
		retMap.put("passwordExpirationDays", this.passwordExpirationDays);
		retMap.put("requireAdminResetForExpiration", this.requireAdminResetForExpiration);
		retMap.put("allowUserChangePassword", this.allowUserChangePassword );
		retMap.put("passReuseCount", this.passReuseCount);
		retMap.put("daysToLock", this.daysToLock);
		retMap.put("daysToLockEmail", this.daysToLockEmail);
		return retMap;
	}

	public boolean validatePassword(String password) {
		boolean isValid = true;
		StringBuilder errorMessage = new StringBuilder();
		if(password.length() < this.minPassLength) {
			errorMessage.append("Password must be at least ").append(this.minPassLength).append(" characters in length.\n");
			isValid = false;
		}
		if(this.requireUpperCase) {
			String upperCaseChars = "(.*[A-Z].*)";
			if(!password.matches(upperCaseChars )) {
				errorMessage.append("Password must have atleast one uppercase character.\n");
				isValid = false;
			}
		}
		if(this.requireLowerCase) {
			String lowerCaseChars = "(.*[a-z].*)";
			if(!password.matches(lowerCaseChars )) {
				errorMessage.append("Password must have atleast one lowercase character.\n");
				isValid = false;
			}
		}
		if(this.requireNumeric) {
			String numbers = "(.*[0-9].*)";
			if(!password.matches(numbers )) {
				System.out.println("Password must have atleast one number");
				isValid = false;
			}
		}
		if(this.requireSpecial) {
			String specialChars = "(.*[!,@,#,$,%,^,&,*].*$)";
			if(!password.matches(specialChars )) {
				errorMessage.append("Password must have atleast one special character among [!,@,#,$,%,^,&,*]");
				isValid = false;
			}
		}
		
		if(!isValid) {
			throw new IllegalArgumentException(errorMessage.toString());
		}
		
		return isValid; 
	}

	public int getMinPassLength() {
		return minPassLength;
	}

	public void setMinPassLength(int minPassLength) {
		this.minPassLength = minPassLength;
	}

	public boolean isRequireUpperCase() {
		return requireUpperCase;
	}

	public void setRequireUpperCase(boolean requireUpperCase) {
		this.requireUpperCase = requireUpperCase;
	}

	public boolean isRequireLowerCase() {
		return requireLowerCase;
	}

	public void setRequireLowerCase(boolean requireLowerCase) {
		this.requireLowerCase = requireLowerCase;
	}

	public boolean isRequireNumeric() {
		return requireNumeric;
	}

	public void setRequireNumeric(boolean requireNumeric) {
		this.requireNumeric = requireNumeric;
	}

	public boolean isRequireSpecial() {
		return requireSpecial;
	}

	public void setRequireSpecial(boolean requireSpecial) {
		this.requireSpecial = requireSpecial;
	}

	public int getPasswordExpirationDays() {
		return passwordExpirationDays;
	}

	public void setPasswordExpirationDays(int passwordExpirationDays) {
		this.passwordExpirationDays = passwordExpirationDays;
	}

	public boolean isRequireAdminResetForExpiration() {
		return requireAdminResetForExpiration;
	}

	public void setRequireAdminResetForExpiration(boolean requireAdminResetForExpiration) {
		this.requireAdminResetForExpiration = requireAdminResetForExpiration;
	}

	public boolean isAllowUserChangePassword() {
		return allowUserChangePassword;
	}

	public void setAllowUserChangePassword(boolean allowUserChangePassword) {
		this.allowUserChangePassword = allowUserChangePassword;
	}

	public int getPassReuseCount() {
		return passReuseCount;
	}

	public void setPassReuseCount(int passReuseCount) {
		this.passReuseCount = passReuseCount;
	}

	public int getDaysToLock() {
		return daysToLock;
	}

	public void setDaysToLock(int daysToLock) {
		this.daysToLock = daysToLock;
	}

	public int getDaysToLockEmail() {
		return daysToLockEmail;
	}

	public void setDaysToLockEmail(int daysToLockEmail) {
		this.daysToLockEmail = daysToLockEmail;
	}

}
