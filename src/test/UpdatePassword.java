package test;

import prerna.auth.utils.SecurityQueryUtils;

public class UpdatePassword {

	public static void main(String[] args) {
		String password = "P@ssw0rd1";
		String salt = SecurityQueryUtils.generateSalt();
		String hashPassword = SecurityQueryUtils.hash(password, salt);
		System.out.println("Salt = " + salt);
		System.out.println("Hash Password = " + hashPassword);
	}
	
}
