package prerna.rpa.hash;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

public class Hasher {
	
	private static final Logger classLogger = LogManager.getLogger(Hasher.class);

	private final int truncateLength; // Bytes
	private final Charset encoding;
	private final MessageDigest digester;

	private static final String DEFAULT_ENCODING = "UTF-8";
	private static final String DEFAULT_ALGORITHM = "SHA-256";
	private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
	
	public static Hasher getDefaultHasher(int truncateLength) {
		try {
			return new Hasher(DEFAULT_ALGORITHM, truncateLength, DEFAULT_ENCODING);
		} catch (NoSuchAlgorithmException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalStateException("The default algorithm should always exist.");
		} catch (UnsupportedEncodingException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalStateException("The default encoding should always be supported.");
		}
	}
	
	public Hasher(String algorithm, int truncateLength, String encoding) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		if (!Charset.isSupported(encoding)) {
			throw new UnsupportedEncodingException();
		}
		if (truncateLength < 1) {
			throw new IllegalArgumentException("Truncate length must be greater than 1.");
		}
		this.truncateLength = truncateLength;
		this.encoding = Charset.forName(encoding);
		digester = MessageDigest.getInstance(algorithm);
	}
	
	public synchronized String hash(String text) {
		byte[] hash = digester.digest(text.getBytes(encoding));

		// Truncate if the hash length is greater than the desired truncated
		// length
		if (hash.length > truncateLength) {
			byte[] truncatedHash = new byte[truncateLength];
			for (int i = 0; i < truncateLength; i++) {
				truncatedHash[i] = hash[i];
			}
			return bytesToHexString(truncatedHash);
		} else {
			return bytesToHexString(hash);
		}
	}
		
	private static String bytesToHexString(byte[] bytes) {

		// See http://stackoverflow.com/q/9655181
		char[] hexChars = new char[bytes.length * 2];
		for (int i = 0; i < bytes.length; i++) {
			int v = bytes[i] & 0xFF;
			hexChars[i * 2] = HEX_ARRAY[v >>> 4];
			hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}
	
}
