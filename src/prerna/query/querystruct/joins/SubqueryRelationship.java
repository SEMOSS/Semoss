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

import java.util.ArrayList;
import java.util.List;
import prerna.query.querystruct.SelectQueryStruct;

public class SubqueryRelationship implements IRelation {

	private SelectQueryStruct qs;
	private String queryAlias;
	private String joinType;
	private List<String[]> joinOnDetails = new ArrayList<>();

	public SubqueryRelationship() {
	}

	public SubqueryRelationship(SelectQueryStruct qs, String queryAlias, String joinType, String[] joinOnDetails) {
		this.qs = qs;
		this.queryAlias = queryAlias;
		this.joinType = joinType;
		this.joinOnDetails.add(joinOnDetails);
	}

	public SubqueryRelationship(SelectQueryStruct qs, String queryAlias, String joinType, List<String[]> joinDetails) {
		this.qs = qs;
		this.queryAlias = queryAlias;
		this.joinType = joinType;
		this.joinOnDetails = joinDetails;
	}

	public SelectQueryStruct getQs() {
		return qs;
	}

	public void setQs(SelectQueryStruct qs) {
		this.qs = qs;
	}

	public String getQueryAlias() {
		return queryAlias;
	}

	public void setQueryAlias(String queryAlias) {
		this.queryAlias = queryAlias;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public void setJoinOnDetails(List<String[]> joinOnDetails) {
		this.joinOnDetails = joinOnDetails;
	}

	public void addJoinOn(String[] joinOn) {
		this.joinOnDetails.add(joinOn);
	}

	public void addJoinOn(String fromConcept, String toConcept, String comparator) {
		if (comparator == null) {
			comparator = "=";
		}
		this.joinOnDetails.add(new String[]{fromConcept, toConcept, comparator});
	}

	public List<String[]> getJoinOnDetails() {
		return this.joinOnDetails;
	}

	@Override
	public RELATION_TYPE getRelationType() {
		return RELATION_TYPE.SUBQUERY;
	}
}
