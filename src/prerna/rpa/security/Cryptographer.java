package prerna.rpa.security;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Random;
import java.util.Scanner;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import prerna.rpa.RPAProps;

// https://stackoverflow.com/a/1133815
public class Cryptographer {
	
	private static final int ITERATION_COUNT = 40000;
	private static final int KEY_LENGTH = 128;
	
	public static void main(String[] args) throws Exception {
		
		// Simulated user input for encrypted properties
		Scanner reader = new Scanner(System.in);
		boolean finished = false;
		while (!finished) {
			
			// Property name
			System.out.println("Enter encrypted property name: ");
			String propertyName = reader.nextLine();
			System.out.println();
			
			// Property value
			System.out.println("Enter encrypted property value: ");
			String propertyValue = reader.nextLine();
			System.out.println();
			
			// Set value
			RPAProps.getInstance().setEncrpytedProperty(propertyName, propertyValue);
			
			// Whether finished
			System.out.println("Finished? (enter y/n): ");
			String finishedString = reader.nextLine();
			System.out.println();
			if (finishedString.equals("y")) {
				finished = true;
			}
		}
		reader.close();
		
		// Flush to file
		RPAProps.getInstance().flushPropertiesToFile();
	}
	
    public static String encrypt(String unprotectedString, String salt, char[] password) throws Exception {
    	return encrypt(unprotectedString, createSecretKey(password, salt.getBytes()));
    }
    
    public static String decrypt(String encryptedString, String salt, char[] password) throws Exception {
    	return decrypt(encryptedString, createSecretKey(password, salt.getBytes()));
    }
    
    // Salt is used to prevent equivalent strings from being encrypted to the same string
	public static String getSalt() {
		Random rand = new Random();
		StringBuilder saltString = new StringBuilder();
		for (int i = 0; i < 10; i ++) {
			saltString.append(rand.nextInt(10));
		}
		return saltString.toString();
	}
	
	// Private helper methods

    private static String encrypt(String unprotectedString, SecretKeySpec key) throws GeneralSecurityException, UnsupportedEncodingException {
        Cipher pbeCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        pbeCipher.init(Cipher.ENCRYPT_MODE, key);
        AlgorithmParameters parameters = pbeCipher.getParameters();
        IvParameterSpec ivParameterSpec = parameters.getParameterSpec(IvParameterSpec.class);
        byte[] cryptoText = pbeCipher.doFinal(unprotectedString.getBytes("UTF-8"));
        byte[] iv = ivParameterSpec.getIV();
        return base64Encode(iv) + ":" + base64Encode(cryptoText);
    }

    private static String decrypt(String encryptedString, SecretKeySpec key) throws GeneralSecurityException, IOException {
        String iv = encryptedString.split(":")[0];
        String property = encryptedString.split(":")[1];
        Cipher pbeCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        pbeCipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(base64Decode(iv)));
        return new String(pbeCipher.doFinal(base64Decode(property)), "UTF-8");
    }
    
    private static SecretKeySpec createSecretKey(char[] password, byte[] salt) throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, ITERATION_COUNT, KEY_LENGTH);
        SecretKey keyTmp = keyFactory.generateSecret(keySpec);
        return new SecretKeySpec(keyTmp.getEncoded(), "AES");
    }
    
    private static String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] base64Decode(String property) throws IOException {
        return Base64.getDecoder().decode(property);
    }
}