package prerna.reactor.security;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jasypt.encryption.pbe.StandardPBEByteEncryptor;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PBEDecryptReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(PBEDecryptReactor.class);

	private static String password = null;
	
	public PBEDecryptReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.QUERY_KEY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		//grab the query
		byte[] encryptedQuery = getInput();
		
		try {
			StandardPBEByteEncryptor encryptor = new StandardPBEByteEncryptor();
			encryptor.setPassword(getPassword());
			byte[] queryBytes = encryptor.decrypt(encryptedQuery);
			String query = new String(queryBytes);
			
			return new NounMetadata(query, PixelDataType.CONST_STRING);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred decrypting with message = " + e.getMessage());
		}
	}

	/**
	 * We allow 2 different input types
	 * Single string input or
	 * Byte [] (passed as array of numbers)
	 * @return
	 */
	private byte[] getInput() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			int size = grs.size();
			if(size == 1) {
				String stringValue = Utility.decodeURIComponent(grs.get(0) + "");
				return stringValue.getBytes();
			} else {
				byte[] arr = new byte[size];
				for(int i = 0; i < size; i++) {
					arr[i] = ((Number) grs.get(i)).byteValue();
				}
				
				return arr;
			}
		}
		
		int size = this.curRow.size();
		if(size == 1) {
			String stringValue = Utility.decodeURIComponent(this.curRow.get(0) + "");
			return stringValue.getBytes();
		} else {
			byte[] arr = new byte[size];
			for(int i = 0; i < size; i++) {
				arr[i] = ((Number) this.curRow.get(i)).byteValue();
			}
			
			return arr;
		}
	}
	
	/**
	 * Get the password from the SMSS file
	 * @return
	 */
	private static String getPassword() {
		if(PBEDecryptReactor.password != null) {
			logger.debug("Decrypting with password >> " + Utility.cleanLogString(PBEDecryptReactor.password));
			return PBEDecryptReactor.password;
		}
		
		synchronized (PBEDecryptReactor.class) {
			if(PBEDecryptReactor.password != null) {
				return PBEDecryptReactor.password;
			}
			
			PBEDecryptReactor.password = DIHelper.getInstance().getProperty(Constants.PM_SEMOSS_EXECUTE_SQL_ENCRYPTION_PASSWORD);
		}
		
		logger.debug("Decrypting with password >> " + Utility.cleanLogString(PBEDecryptReactor.password));
		return PBEDecryptReactor.password;
	}
	
	
	public static void main(String[] args) {
		String value = "SELECT * FROM MOVIE_DATA";
		
		StandardPBEByteEncryptor encryptor = new StandardPBEByteEncryptor();
		encryptor.setPassword("password123");
		byte[] queryBytes = encryptor.encrypt(value.getBytes());
		String encoded = new String(queryBytes);
		System.out.println("Encode array > " + Arrays.toString(queryBytes));
		System.out.println("Encoded >> " + encoded);
		
//		byte[] decodedBytes = encryptor.decrypt(queryBytes);
//		String decoded = new String(decodedBytes);
//		System.out.println("Decoded >> " + decoded);
	}
	
}
