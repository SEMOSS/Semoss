package prerna.reactor.insights.save;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.ZoneId;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AddInsightAPIReactor extends AbstractReactor {

	public AddInsightAPIReactor() {
		
		// the current user
		// project id
		// insight id
		// password - uses project id to convert into hash / api key
		// created on - current date and time
		// limit 
		// count
		// expires _on - default is 5 days after API
		// consumer
		
		// limit - if not set goes to 
		this.keysToGet = new String[]{
									  ReactorKeysEnum.PROJECT.getKey(), 
									  ReactorKeysEnum.ID.getKey(), 
									  ReactorKeysEnum.PASSWORD.getKey(),
									  ReactorKeysEnum.CONSUMER_ID.getKey(),
									  ReactorKeysEnum.LIMIT.getKey(),
									  ReactorKeysEnum.EXPIRES_ON.getKey()
									};
		
		this.keyRequired = new int[] {
							1,
							1,
							1,
							1,
							0,
							0
				
		};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		String author = null;
		String email = user.getAccessToken(user.getPrimaryLogin()).getEmail();
		String id = user.getAccessToken(user.getPrimaryLogin()).getId();
		
		if(AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		organizeKeys();
		
		String projectId = keyValue.get(keysToGet[0]);
		String insightId = keyValue.get(keysToGet[1]);
		String pass = keyValue.get(keysToGet[2]);
		
		// right now we dont validate with list of existing consumer id
		// if we want eventually we will validate this to make sure you cannot add random consumers
		String consumerId = keyValue.get(keysToGet[3]);

		
		int limit = 100;
		if(keyValue.containsValue(keysToGet[4]))
			limit = Integer.parseInt(keyValue.get(keysToGet[4]));
		
		LocalDate dt = LocalDate.now();
		java.util.Date utilToday = Date.from(dt.atStartOfDay(ZoneId.systemDefault()).toInstant());
		Date createdOn = new Date(utilToday.getTime());
		Time createdTime = new Time(utilToday.getTime());
		
		int count = 0; // initialized to zero
		
		dt = dt.plusDays(100); // default expiry at 100 days
		java.util.Date utilExpire = Date.from(dt.atStartOfDay(ZoneId.systemDefault()).toInstant());
		//Date expiresOn = new Date(utilExpire.getTime());
		Time expiresTime = new Time(utilExpire.getTime());
		String apiKey = null;
		try {
			// generate the API key
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			String finalData = projectId + insightId + pass;
			byte[] digest = md.digest(finalData.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < digest.length; i++) {
				sb.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
			}
			
			apiKey = sb.toString();
			
			// find if this project insight consumer already exists
			// if so throw an error
			String existingSQL = "SELECT count(*) from API_KEY where Project_ID = '" + projectId + "\' and INSIGHT_ID = '" + insightId + "' and CONSUMER_ID = '" + consumerId + "'";
			IRDBMSEngine secDB = (IRDBMSEngine)Utility.getDatabase(Constants.SECURITY_DB);
			
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(secDB, existingSQL);
			
			if(wrapper.getNumRecords() > 0)
			{
				// throw an exception to say you need to use the update insight APi
			}
			
			// first field is the database
			String [] colNames = new String[] {"API_KEY", "CREATOR_ID", "PROJECT_ID", "INSIGHT_ID", "API_KEY", "CREATED_ON", "API_LIMIT", "COUNT", "EXPIRES_ON","CONSUMER_ID"};

			PreparedStatement pst = secDB.bulkInsertPreparedStatement(colNames);
			pst.setString(1, id);
			pst.setString(2, projectId);
			pst.setString(3, insightId);
			pst.setString(4, apiKey);
			pst.setDate(5, createdOn);
			pst.setInt(6, limit);
			pst.setInt(7, 0);
			pst.setTime(8, expiresTime);
			pst.setString(9, consumerId);
			pst.execute();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new NounMetadata("Completeled insight API <<" + apiKey + ">>", PixelDataType.CONST_STRING);
	}

}
