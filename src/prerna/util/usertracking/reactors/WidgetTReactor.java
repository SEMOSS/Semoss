package prerna.util.usertracking.reactors;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import prerna.auth.User;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class WidgetTReactor extends AbstractReactor {

	private static final String WIDGET_CHANGE_KEY = "WIDGET_CHANGE";
	private static final String WIDGET_NAME_KEY = "WIDGET_NAME";

	public WidgetTReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		List<Object[]> rows = new Vector<Object[]>();
		
		IUserTracker tracker = UserTrackerFactory.getInstance();
		if(tracker.isActive()) {
			GenRowStruct grs = getGrs();
			
			String id = UUID.randomUUID().toString();
			String sessionId = null;
			String userId = null;
			// FE runs this on an empty insight
			// so they are passing me an insight id that is being droped
			// but i should be able to user the correct session id and user id from the empty insight
			sessionId = AbstractSqlQueryUtil.escapeForSQLStatement(ThreadStore.getSessionId());
			User user = this.insight.getUser();
			if(user != null) {
				if(!user.getLogins().isEmpty()) {
					userId = user.getAccessToken(user.getLogins().get(0)).getId();
				} else if(user.isAnonymous()) {
					userId = user.getAnonymousId();
				}
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
					insightId = AbstractSqlQueryUtil.escapeForSQLStatement(m.get("insightID").toString());
					time = m.get("time").toString();
					if(m.get("change") != null  && m.get("change") instanceof Map) {
						Map inMap = (Map) m.get("change");
						subType = inMap.get("type").toString();
						value = inMap.get("value").toString();
					} else {
						subType = m.get("type").toString();
						value = m.get("value").toString();
					}
					
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
			
			tracker.trackUserWidgetMods(rows);
		}
		
		NounMetadata noun;
		if(rows.size() > 0) {
			noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		} else {
			noun = new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		
		return noun;
	}

	private GenRowStruct getGrs() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null) {
			return grs;
		}
		
		return this.curRow;
	}
	
}
