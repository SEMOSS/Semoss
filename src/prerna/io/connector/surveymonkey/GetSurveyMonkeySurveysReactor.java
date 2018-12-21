package prerna.io.connector.surveymonkey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class GetSurveyMonkeySurveysReactor extends AbstractReactor{

	@Override
	public NounMetadata execute() {
		String url_str = "https://api.surveymonkey.com/v3/surveys";
		List<Map<String, Object>> masterList = new ArrayList<Map<String, Object>>();

		String [] beanProps = {"id", "name"};
		String jsonPattern = "data[].{id: id, name: title}";

		//get access token
		String accessToken=null;
		User user = this.insight.getUser();
		try{
			if(user == null){
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "surveymonkey");
				retMap.put("message", "Please login to your Survey Monkey account");
				throwLoginError(retMap);
			}
			else if (user != null) {
				AccessToken token = user.getAccessToken(AuthProvider.SURVEYMONKEY);
				accessToken = token.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "surveymonkey");
			retMap.put("message", "Please login to your Survey Monkey account");
			throwLoginError(retMap);
		}


		// query params
		Hashtable params = new Hashtable();
		params.put("per_page", 1000);
		params.put("sort_order", "DESC");
		// make the call
		String output = AbstractHttpHelper.makeGetCall(url_str, accessToken, params, true);
		
		// fill the bean with the return
		Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		if(C instanceof RemoteItem){
			RemoteItem fileList= (RemoteItem) C;
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", fileList.getName());
			tempMap.put("id", fileList.getId());
			masterList.add(tempMap);
		}
		else{
			List<RemoteItem> fileList = (List<RemoteItem>) C;
			for(RemoteItem entry : fileList){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				masterList.add(tempMap);
			}
		}
		
		return new NounMetadata(masterList, PixelDataType.MAP);
	}
}
