package prerna.sablecc2.reactor.security;

import org.jasypt.encryption.pbe.StandardPBEByteEncryptor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PBEDecryptReactor extends AbstractReactor {

	private static String password = null;
	
	public PBEDecryptReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.QUERY_KEY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		//grab the query
		String encryptedQuery = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[0]));
		
		StandardPBEByteEncryptor encryptor = new StandardPBEByteEncryptor();
		encryptor.setPassword(getPassword());
		byte[] queryBytes = encryptor.decrypt(encryptedQuery.getBytes());
		String query = new String(queryBytes);
		
		return new NounMetadata(query, PixelDataType.CONST_STRING);
	}

	/**
	 * Get the password from the SMSS file
	 * @return
	 */
	private static String getPassword() {
		if(PBEDecryptReactor.password != null) {
			return PBEDecryptReactor.password;
		}
		
		synchronized (PBEDecryptReactor.class) {
			if(PBEDecryptReactor.password != null) {
				return PBEDecryptReactor.password;
			}
			
			PBEDecryptReactor.password = DIHelper.getInstance().getProperty(Constants.PM_SEMOSS_EXECUTE_SQL_ENCRYPTION_PASSWORD);
		}
		
		return PBEDecryptReactor.password;
	}
	
	
	public static void main(String[] args) {
		String value = "SELECT * FROM MOVIE_DATA";
		
		StandardPBEByteEncryptor encryptor = new StandardPBEByteEncryptor();
		encryptor.setPassword("password123");
		byte[] queryBytes = encryptor.encrypt(value.getBytes());
		String encoded = new String(queryBytes);
		System.out.println("Encoded >> " + encoded);
		
//		byte[] decodedBytes = encryptor.decrypt(queryBytes);
//		String decoded = new String(decodedBytes);
//		System.out.println("Decoded >> " + decoded);
	}
	
}
