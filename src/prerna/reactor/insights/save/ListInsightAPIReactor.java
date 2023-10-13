package prerna.reactor.insights.save;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ListInsightAPIReactor extends AbstractReactor {

	public ListInsightAPIReactor() {
		
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
									};
		
		this.keyRequired = new int[] {
							1,
							1,
		};
	}

	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		String projectId = keyValue.get(keysToGet[0]);
		String insightId = keyValue.get(keysToGet[1]);
		
		Map <String, String> consumerAPIMap = new HashMap<String, String>();
		try {
			// generate the API key
			
			// find if this project insight consumer already exists
			// if so throw an error
			String existingSQL = "SELECT CONSUMER_ID, API_KEY, DISABLED from API_KEY where Project_ID = '" + projectId + "\' and INSIGHT_ID = '" + insightId + "'";
			IRDBMSEngine secDB = (IRDBMSEngine)Utility.getDatabase(Constants.SECURITY_DB);
			
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(secDB, existingSQL);
			
			
			while(wrapper.hasNext())
			{
				// throw an exception to say you need to use the update insight APi
				IHeadersDataRow hdr = wrapper.next();
				Object [] values = hdr.getValues();
				String consumerId = values[0] + "";
				String api = values[1] + "";
				boolean disabled = (Boolean)values[2];
				if(disabled)
					consumerId = "Disabled  <" + consumerId + ">";
				consumerAPIMap.put(consumerId, api);
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
		
		return new NounMetadata(consumerAPIMap, PixelDataType.MAP);
	}

}
