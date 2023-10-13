package prerna.reactor.insights.save;

import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

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

public class DisableInsightAPIReactor extends AbstractReactor {

	public DisableInsightAPIReactor() {
		
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
										ReactorKeysEnum.API_KEY.getKey()
									};
		
		this.keyRequired = new int[] {
				1
		};
	}

	@Override
	public NounMetadata execute() 
	{
		// can delete the key only if admin or the creator
		User user = this.insight.getUser();
		String author = null;
		String email = user.getAccessToken(user.getPrimaryLogin()).getEmail();
		
		
		if(AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		organizeKeys();
		
		String apiKey = keyValue.get(keysToGet[0]);
				
		try {
			String existingSQL = "SELECT CREATOR_ID from API_KEY where API_KEY = '" + apiKey + "'";
			IRDBMSEngine secDB = (IRDBMSEngine)Utility.getDatabase(Constants.SECURITY_DB);
			
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(secDB, existingSQL);
			boolean foundUser = false;
			
			while(wrapper.hasNext() && !foundUser)
			{
				// throw an exception to say you need to use the update insight APi
				String thisCreator = wrapper.next().getValues()[0] + "";
				foundUser = thisCreator.equalsIgnoreCase(email);
			}
			if(!foundUser)
				return new NounMetadata("Unauthorized user for this API KEY", PixelDataType.CONST_STRING);
			
			// update to disabled
			// with disabled on
			if(foundUser)
			{
				String sql = "UPDATE API_KEY SET DISABLED=?, DISABLED_ON=? WHERE API_KEY=?";
				
				PreparedStatement pst = secDB.getPreparedStatement(sql);				
				pst.setBoolean(1, true);
				pst.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
				pst.setString(3, apiKey);
				pst.execute();
			}
			
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
		
		return new NounMetadata("Deleted API Key " + apiKey, PixelDataType.CONST_STRING);
	}

}
