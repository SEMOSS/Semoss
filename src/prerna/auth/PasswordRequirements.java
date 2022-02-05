package prerna.auth;

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
	private int maxPasswordReuse = -1;

	private PasswordRequirements() {
		loadRequirements();
	}

	public static PasswordRequirements getInstance() {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(PasswordRequirements.class) {
				if(instance != null) {
					return instance;
				}

				instance = new PasswordRequirements();
			}
		}

		return instance;
	}

	private void loadRequirements() {
		// pull the necessary details

	}

	public boolean validatePassword(User user, String password) {
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

	public int getMaxPasswordReuse() {
		return maxPasswordReuse;
	}

	public void setMaxPasswordReuse(int maxPasswordReuse) {
		this.maxPasswordReuse = maxPasswordReuse;
	}

}
