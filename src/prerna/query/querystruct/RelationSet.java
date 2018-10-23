package prerna.query.querystruct;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class RelationSet extends LinkedHashSet<String[]> {

	@Override
	public boolean add(String[] e) {
		Iterator<String[]> it = this.iterator();
		while(it.hasNext()) {
			String[] values = it.next();
			if(values[0].equals(e[0])
					&& values[1].equals(e[1])
					&& values[2].equals(e[2])
					) {
				return false;
			}
		}
		return super.add(e);
	}
	
	public static void main(String[] args) {
		Set<String[]> values = new RelationSet();
		values.add(new String[]{"a","b","c"});
		values.add(new String[]{"a","b","c"});
		values.add(new String[]{"a","b","c"});

		List<String[]> valuesList = new Vector<String[]>();
		valuesList.add(new String[]{"a", "b", "c"});
		values.addAll(valuesList);
		
		System.out.println(values);
	}
}
