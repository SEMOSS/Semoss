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
package prerna.sablecc2.om;

// left column
// right column
// type of join
public class Join {

	// public enum JOIN_TYPE {INNER, OUTER, RIGHT_OUTER, LEFT_OUTER, CROSS_JOIN,
	// SELF_JOIN};
	// there is no reason I cannot split this into 2 different classes other than
	// laziness --it has been split

	private String joinType = null;
	private String lColumn = null;
	private String rColumn = null;
	// default comparator is =
	private String comparator = "==";
	private String joinRelName = null;

	public Join(String lCol, String joinType, String rCol) {
		this.lColumn = lCol;
		this.rColumn = rCol;
		this.joinType = joinType;
	}

	public Join(String lCol, String joinType, String rCol, String comparator, String joinRelName) {
		this.lColumn = lCol;
		this.rColumn = rCol;
		this.joinType = joinType;
		this.comparator = comparator;
		this.joinRelName = joinRelName;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getLColumn() {
		return lColumn;
	}

	public void setLColumn(String lColumn) {
		this.lColumn = lColumn;
	}

	public String getRColumn() {
		return rColumn;
	}

	public void setRColumn(String rColumn) {
		this.rColumn = rColumn;
	}

	public String getComparator() {
		return comparator;
	}

	public void setComparator(String comparator) {
		this.comparator = comparator;
	}

	public String getJoinRelName() {
		return joinRelName;
	}

	public void getJoinRelName(String joinRelName) {
		this.joinRelName = joinRelName;
	}
}
