package prerna.solr.reactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.sql.AbstractSqlQueryUtil;

public class MyAppsReactor extends AbstractReactor {

	private static List<String> META_KEYS_LIST = new Vector<String>();
	static {
		META_KEYS_LIST.add("description");
		META_KEYS_LIST.add("tag");
	}
	
	public MyAppsReactor() {

	}

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> appInfo = new Vector<Map<String, Object>>();

		if(AbstractSecurityUtils.securityEnabled()) {
			appInfo = SecurityQueryUtils.getUserDatabaseList(this.insight.getUser());
		} else {
			appInfo = SecurityQueryUtils.getAllDatabaseList();
		}
		
		int size = appInfo.size();
		Map<String, Integer> index = new HashMap<String, Integer>(appInfo.size());
		// now we want to add most executed insights
		for(int i = 0; i < size; i++) {
			Map<String, Object> app = appInfo.get(i);
			String appId = app.get("app_id").toString();
			SemossDate lmDate = SecurityQueryUtils.getLastExecutedInsightInApp(appId);
			// could be null when there are no insights in an app
			if(lmDate != null) {
				app.put("lastModified", lmDate.getFormattedDate());
			}
			
			// just going to init for FE
			app.put("description", "");
			app.put("tags", new Vector<String>());
			
			// keep list of app ids to get the index
			index.put(appId, new Integer(i));
		}
		
		IRawSelectWrapper wrapper = SecurityAppUtils.getAppMetadataWrapper(index.keySet(), META_KEYS_LIST);
		while(wrapper.hasNext()) {
			Object[] data = wrapper.next().getValues();
			String appId = (String) data[0];

			String metaKey = (String) data[1];
			String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) data[2]);
			if(value == null) {
				continue;
			}
			
			int indexToFind = index.get(appId);
			Map<String, Object> res = appInfo.get(indexToFind);
			// right now only handling description + tags
			if(metaKey.equals("description")) {
				// we only have 1 description per insight
				// so just push
				res.put("description", value);
			} else if(metaKey.equals("tag")){
				// multiple tags per insight
				List<String> tags = (List<String>) res.get("tags");;
				// add to the list
				tags.add(value);
			}
		}
		
		return new NounMetadata(appInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

}
