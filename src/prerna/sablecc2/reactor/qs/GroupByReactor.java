package prerna.sablecc2.reactor.qs;

import prerna.ds.h2.H2Frame;
import prerna.ds.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class GroupByReactor extends QueryStructReactor
{

	//TODO : if a type does not match throw syntax exception, we are getting something we are not expecting
	QueryStruct2 createQueryStruct() {
		//get the selectors
		GenRowStruct allNouns = curRow;//getNounStore().getNoun(NounStore.all); //must only be strings
		
		if(isDatabaseQueryStruct()) {
			for(int selectIndex = 0;selectIndex < allNouns.size();selectIndex++) {
				String thisSelector = (String)allNouns.get(selectIndex);
				if(thisSelector.contains("__")){
					String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
					String property = thisSelector.substring(thisSelector.indexOf("__")+2);
					qs.addGroupBy(concept, property);
				}
				else {
					qs.addGroupBy(thisSelector, null);
				}
			}
		} else {
			H2Frame frame = (H2Frame)planner.getFrame();
			String tableName = frame.getBuilder().getTableName();
			for(int selectIndex = 0;selectIndex < allNouns.size();selectIndex++) {
				String thisSelector = (String)allNouns.get(selectIndex);				
				qs.addGroupBy(tableName, thisSelector);
			}
		}
		return qs;
	}
	
	//determine whether this query struct will be built for a database or a frame
	private boolean isDatabaseQueryStruct() {
		QueryStruct2 struct = (QueryStruct2)planner.getProperty("QUERYSTRUCT", "QUERYSTRUCT");
		if(struct == null) {
			return false;
		} else {
			if(struct.getEngineName() == null) {
				return false;
			}
		}
		return true;
	}
}
