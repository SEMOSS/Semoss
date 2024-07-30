package prerna.io.connector.secrets;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jodd.util.BCrypt;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.InsightEncryptionException;
import prerna.util.Constants;

public class SecretsUtility {

	private static final Logger logger = LogManager.getLogger(SecretsUtility.class);

	private SecretsUtility() {
		
	}
	
	public static Cipher generateCipherForInsight(String projectId, String projectName, String insightId) {
		ISecrets secretsEngine = SecretsFactory.getSecretConnector();
		if(secretsEngine == null) {
			throw new InsightEncryptionException("Encryption services have not been enabled on this instance. Caching will not occur for this insight");
		}
		
		String secret = UUID.randomUUID().toString();
		String salt = BCrypt.gensalt();
		byte[] iv = new byte[16];
		Cipher cipher = null;
		try {
			SecureRandom randomSecureRandom = new SecureRandom();
			randomSecureRandom.nextBytes(iv);
			IvParameterSpec ivspec = new IvParameterSpec(iv);
			
			SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
			KeySpec spec = new PBEKeySpec(secret.toCharArray(), salt.getBytes(), 65536, 256);
			SecretKey tmp = factory.generateSecret(spec);
			SecretKeySpec secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");

			cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivspec);
		} catch (NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException | NoSuchPaddingException | InvalidKeySpecException e1) {
			logger.error(Constants.STACKTRACE, e1);
		}
		if(cipher == null) {
			throw new InsightEncryptionException("Unable to generate encryption details for the insight cache");
		}
		
		Map<String, Object> cacheData = new HashMap<>();
		cacheData.put(ISecrets.SECRET, secret);
		cacheData.put(ISecrets.SALT, salt);
		cacheData.put(ISecrets.IV, iv);
		secretsEngine.writeInsightEncryptionSecrets(projectId, projectName, insightId, cacheData);
		return cipher;
	}
	
	public static Cipher retrieveCipherForInsight(Insight in) {
		return retrieveCipherForInsight(in.getProjectId(), in.getProjectName(), in.getRdbmsId());
	}
	
	public static Cipher retrieveCipherForInsight(String projectId, String projectName, String insightId) {
		ISecrets secretsEngine = SecretsFactory.getSecretConnector();
		if(secretsEngine == null) {
			throw new InsightEncryptionException("Encryption services have not been enabled on this instance. Cannot retrieve details to decrypt the insight");
		}
		
		Map<String, Object> cacheData = secretsEngine.getInsightEncryptionSecrets(projectId, projectName, insightId);
		String secret = (String) cacheData.get(ISecrets.SECRET);
		String salt = (String) cacheData.get(ISecrets.SALT);
		byte[] iv = (byte[]) cacheData.get(ISecrets.IV);
		Cipher cipher = null;
		try {
			IvParameterSpec ivspec = new IvParameterSpec(iv);
			
			SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
			KeySpec spec = new PBEKeySpec(secret.toCharArray(), salt.getBytes(), 65536, 256);
			SecretKey tmp = factory.generateSecret(spec);
			SecretKeySpec secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");

			cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
			cipher.init(Cipher.DECRYPT_MODE, secretKey, ivspec);
		} catch (NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException | NoSuchPaddingException | InvalidKeySpecException e1) {
			logger.error(Constants.STACKTRACE, e1);
		}
		if(cipher == null) {
			throw new InsightEncryptionException("Unable to generate encryption details for the insight cache");
		}
		
		return cipher;
	}
	
}
