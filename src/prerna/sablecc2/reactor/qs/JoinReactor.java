package prerna.sablecc2.reactor.qs;

import prerna.ds.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounStore;

public class JoinReactor extends QueryStructReactor {

	@Override
	QueryStruct2 createQueryStruct() {
		GenRowStruct joins = getNounStore().getNoun(NounStore.all);
		for(int i = 0; i < joins.size(); i++) {
			if(joins.get(i) instanceof Join) {
				Join join = (Join)joins.get(i);
				qs.addRelation(join.getSelector(), join.getQualifier(), join.getJoinType());
			}
		}
		return qs;
	}
}
