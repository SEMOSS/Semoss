package prerna.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

public class JoinReactor extends AbstractQueryStructReactor {
	
	public JoinReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.JOINS.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		GenRowStruct joins = getNounStore().getNoun(NounStore.all);
		for (int i = 0; i < joins.size(); i++) {
			if (joins.get(i) instanceof Join) {
				Join join = (Join) joins.get(i);
				qs.addRelation(join.getLColumn(), join.getRColumn(), join.getJoinType(), 
						join.getComparator(), join.getJoinRelName());
			}
		}

		return qs;
	}
}
