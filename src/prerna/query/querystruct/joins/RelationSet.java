package prerna.query.querystruct.joins;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RelationSet extends LinkedHashSet<IRelation> {

	@Override
	public boolean add(IRelation e) {
		Iterator<IRelation> it = this.iterator();
		while(it.hasNext()) {
			IRelation values = it.next();
			if(values.equals(e)) {
				return false;
			}
		}
		return super.add(e);
	}
	
//	public static void main(String[] args) {
//		Set<IRelation> values = new RelationSet();
//		values.add(new BasicRelationship(new String[]{"a","b","c"}));
//		values.add(new BasicRelationship(new String[]{"a","b","c"}));
//		values.add(new BasicRelationship(new String[]{"a","b","c"}));
//
//		List<IRelation> valuesList = new ArrayList<>();
//		valuesList.add(new BasicRelationship(new String[]{"a", "b", "c"}));
//		values.addAll(valuesList);
//		
//		System.out.println(values);
//	}
}
