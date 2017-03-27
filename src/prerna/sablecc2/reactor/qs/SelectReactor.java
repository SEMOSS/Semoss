package prerna.sablecc2.reactor.qs;


import prerna.ds.h2.H2Frame;
import prerna.ds.querystruct.QueryStruct2;
import prerna.ds.querystruct.QueryStructSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;

public class SelectReactor extends QueryStructReactor {

	/**
	 * 
	 * @return
	 * 
	 * 
	 */
	//TODO : if a type does not match throw syntax exception, we are getting something we are not expecting
	QueryStruct2 createQueryStruct() {
		//get the selectors
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //can either be strings or column selectors
		
		//if the query struct is supposed to be used to query a database
		//selectors are as such:
		//concept is "concept"
		//property is "concept__property" where concept is the parent, property is the column name of the property
		if(isDatabaseQueryStruct()) {
			for(int selectIndex = 0;selectIndex < allNouns.size();selectIndex++) {
				String thisSelector = (String)allNouns.get(selectIndex);
				if(thisSelector.contains("__")){
					String concept = thisSelector.substring(0, thisSelector.indexOf("__"));
					String property = thisSelector.substring(thisSelector.indexOf("__")+2);
					QueryStructSelector selector = getSelector(concept, property);
					qs.addSelector(selector);
				}
				else {
					qs.addSelector(thisSelector, null);
				}
			}
		} 
		
		//Otherwise we are going to use the query struct for a frame in which case
		//EVERY column will be referenced as Table__Column
		else {
			H2Frame frame = (H2Frame)planner.getFrame();
			String tableName = frame.getBuilder().getTableName();
			for(int selectIndex = 0;selectIndex < allNouns.size();selectIndex++) {
				if(allNouns.get(selectIndex) instanceof String) {
					String thisSelector = (String)allNouns.get(selectIndex);
					QueryStructSelector selector = getSelector(tableName, thisSelector);
					qs.addSelector(selector);
				} 
				else if(allNouns.get(selectIndex) instanceof QueryStructSelector) {
					QueryStructSelector thisSelector = (QueryStructSelector)allNouns.get(selectIndex);
					this.qs.addSelector(thisSelector);
				}
			}
		}
		return qs;
	}
	
	public QueryStructSelector getSelector(String table, String column) {
		QueryStructSelector selector = new QueryStructSelector();
		selector.setTable(table);
		if(column == null) {
			selector.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
		} else {
			selector.setColumn(column);
		}
		return selector;
	}
	
	//determine whether this query struct will be built for a database or a frame
	private boolean isDatabaseQueryStruct() {
		GenRowStruct result = this.store.getNoun("QUERYSTRUCT");
		if(result != null && result.getMeta(0).toString().equals("QUERYSTRUCT")) {
			NounMetadata storedResult = (NounMetadata)result.get(0);
			if(storedResult.getValue() instanceof QueryStruct2) {
				
				//if no engine name in query struct, we will use it for a frame
				if( ((QueryStruct2)storedResult.getValue()).getEngineName() == null) {
					return false;
				} else {
					return true;
				}
			} else {
				
				//if we have no query struct, we will default to this being used on a frame
				return false;
			}
		}
		//if we have no query struct, we will default to this being used on a frame
		return false;
	}	
}
