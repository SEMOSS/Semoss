package prerna.util.usertracking.reactors;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.job.JobReactor;

public class WidgetTrackingReactor extends AbstractReactor {

	private static final String WIDGET_CHANGE_KEY = "WIDGET_CHANGE";
	private static final String WIDGET_NAME_KEY = "WIDGET_NAME";

	public WidgetTrackingReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		GenRowStruct grs = getGrs();
		List<Object[]> rows = new Vector<Object[]>();
		
		String id = UUID.randomUUID().toString();
		String sessionId = null;
		String userId = null;
		// FE runs this on an empty insight
		// so they are passing me an insight id that is being droped
		// but i should be able to user the correct session id and user id from the empty insight
		VarStore vStore = this.insight.getVarStore();
		sessionId = RdbmsQueryBuilder.escapeForSQLStatement(vStore.get(JobReactor.SESSION_KEY).getValue().toString());
		User user = this.insight.getUser();
		if(user != null) {
			userId = user.getAccessToken(user.getLogins().get(0)).getId();
			userId = RdbmsQueryBuilder.escapeForSQLStatement(userId);
		}
		
		
		int size = grs.size();
		for(int i = 0; i < size; i++) {
			Object mObj = grs.get(i);
			if(mObj instanceof Map) {
				// values to grab
				String insightId = null;
				String time = null;
				String subType = null;
				String value = null;
				
				// get the values
				Map m = (Map) mObj;
				insightId = RdbmsQueryBuilder.escapeForSQLStatement(m.get("insightID").toString());
				time = m.get("time").toString();
				//TODO: have FE remove this so i can avoid these checks
				if(time.contains("T")) {
					time = time.replace('T', ' ');
				}
				if(time.contains("Z")) {
					time = time.replace("Z", "");
				}
				Map inMap = (Map) m.get("change");
				subType = inMap.get("type").toString();
				value = inMap.get("value").toString();
				
				// generate row
				Object[] row = new Object[15];
				row[0] = id;
				// input type
				row[7] = WIDGET_CHANGE_KEY;
				// input subtype
				row[8] = subType;
				// input name
				row[9] = WIDGET_NAME_KEY;
				// input value
				row[10] = value;
				// session id
				row[11] = sessionId;
				// insight id
				row[12] = insightId;
				// user id
				row[13] = userId;
				// time
				row[14] = time;
				// add batch
				rows.add(row);
			}
		}
		
		
		
		return null;
	}

	private GenRowStruct getGrs() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null) {
			return grs;
		}
		
		return this.curRow;
	}
	
}
