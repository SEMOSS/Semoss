/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.query.querystruct.joins;

import java.util.Iterator;
import java.util.LinkedHashSet;

public class RelationSet extends LinkedHashSet<IRelation> {

	@Override
	public boolean add(IRelation e) {
		Iterator<IRelation> it = this.iterator();
		while (it.hasNext()) {
			IRelation values = it.next();
			if (values.equals(e)) {
				return false;
			}
		}
		return super.add(e);
	}

	// public static void main(String[] args) {
	// Set<IRelation> values = new RelationSet();
	// values.add(new BasicRelationship(new String[]{"a","b","c"}));
	// values.add(new BasicRelationship(new String[]{"a","b","c"}));
	// values.add(new BasicRelationship(new String[]{"a","b","c"}));
	//
	// List<IRelation> valuesList = new ArrayList<>();
	// valuesList.add(new BasicRelationship(new String[]{"a", "b", "c"}));
	// values.addAll(valuesList);
	//
	// System.out.println(values);
	// }
}
