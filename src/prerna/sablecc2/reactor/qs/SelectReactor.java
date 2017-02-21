package prerna.sablecc2.reactor.qs;


import prerna.ds.QueryStruct;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class SelectReactor extends QueryStructReactor {

	/**
	 * 
	 * @return
	 * 
	 * 
	 */
	//TODO : if a type does not match throw syntax exception, we are getting something we are not expecting
	QueryStruct createQueryStruct() {
		//get the selectors
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //can either be strings or column selectors
		
		if(isDatabaseQueryStruct()) {
			for(int selectIndex = 0;selectIndex < allNouns.size();selectIndex++) {
				String thisSelector = (String)allNouns.get(selectIndex);
				if(thisSelector.contains("__")){
					String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
					String property = thisSelector.substring(thisSelector.indexOf("__")+2);
					qs.addSelector(concept, property);
				}
				else {
					qs.addSelector(thisSelector, null);
				}
			}
		} else {
			H2Frame frame = (H2Frame)planner.getFrame();
			String tableName = frame.getBuilder().getTableName();
			for(int selectIndex = 0;selectIndex < allNouns.size();selectIndex++) {
				if(allNouns.get(selectIndex) instanceof String) {
					String thisSelector = (String)allNouns.get(selectIndex);				
					qs.addSelector(tableName, thisSelector);
				} else if(allNouns.get(selectIndex) instanceof QueryStructSelector) {
//					QueryStructSelector thisSelector = (QueryStructSelector)allNouns.get(selectIndex);
//					String column = thisSelector.getColumn();
//					String math = thisSelector.getMath();
//					String alias = thisSelector.getAlias();
//					String mathSelector = math+"("+column+")";
				}
			}
		}
		return qs;
	}
	
	//determine whether this query struct will be built for a database or a frame
	private boolean isDatabaseQueryStruct() {
		QueryStruct struct = (QueryStruct)planner.getProperty("QUERYSTRUCT", "QUERYSTRUCT");
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
