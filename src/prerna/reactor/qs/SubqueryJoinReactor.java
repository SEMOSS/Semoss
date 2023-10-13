package prerna.reactor.qs;

import java.util.ArrayList;
import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.sablecc2.om.GenRowStruct;

public class SubqueryJoinReactor extends AbstractQueryStructReactor {
	
	private static final String SUB_QS = "subQs";
	private static final String SUB_QS_ALIAS = "subQsAlias";
	private static final String ON = "on";
	private static final String JOIN_TYPE = "jType";
	
	public SubqueryJoinReactor() {
		this.keysToGet = new String[]{SUB_QS, SUB_QS_ALIAS, ON, JOIN_TYPE};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		SelectQueryStruct subQs = getSubQs();
		String subQsAlias = getSubQsAlias();
		List<String[]> joinOn = getJoinOn();
		String jType = getJoinType();
		
		SubqueryRelationship rel = new SubqueryRelationship(subQs, subQsAlias, jType, joinOn);
		qs.addRelation(rel);
		return qs;
	}

	private String getJoinType() {
		String jType = null;
		GenRowStruct grs = this.store.getNoun(JOIN_TYPE);
		if(grs != null && !grs.isEmpty()) {
			jType = (String) grs.get(0);
		}
		
		if(jType == null || jType.isEmpty()) {
			throw new IllegalArgumentException("Must define the join type for the inner subquery");
		}
		return jType;
	}

	private List<String[]> getJoinOn() {
		List<String[]> joinOns = new ArrayList<>();

		GenRowStruct grs = this.store.getNoun(ON);
		if(grs != null && !grs.isEmpty()) {
			if(grs.size() % 3 != 0) {
				throw new IllegalArgumentException("Must define the join on statement to be of the for [from, to, comparator]");
			}
			for(int i = 0; i < grs.size(); i=i+3) {
				String from = (String) grs.get(i);
				String to = (String) grs.get(i+1);
				String comparator = (String) grs.get(i+2);
				joinOns.add(new String[] {from,to,comparator});				
			}
		}
		
		if(joinOns == null || joinOns.isEmpty()) {
			throw new IllegalArgumentException("Must define what the subquery is joined on to the base query");
		}
		
		return joinOns;
	}

	private String getSubQsAlias() {
		String subQsAlias = null;
		GenRowStruct grs = this.store.getNoun(SUB_QS_ALIAS);
		if(grs != null && !grs.isEmpty()) {
			subQsAlias = (String) grs.get(0);
		}
		
		if(subQsAlias == null || subQsAlias.isEmpty()) {
			throw new IllegalArgumentException("Must define the inner subquery alias for the join");
		}
		
		return subQsAlias;
	}

	private SelectQueryStruct getSubQs() {
		GenRowStruct grs = this.store.getNoun(SUB_QS);
		if(grs != null && !grs.isEmpty()) {
			return (SelectQueryStruct) grs.get(0);
		}
		
		throw new IllegalArgumentException("Must define the inner subquery for the join");
	}
}
