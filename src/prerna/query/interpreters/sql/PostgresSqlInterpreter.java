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
package prerna.query.interpreters.sql;

import java.util.List;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;

public class PostgresSqlInterpreter extends SqlInterpreter {

	public PostgresSqlInterpreter() {
	}

	public PostgresSqlInterpreter(IDatabaseEngine engine) {
		super(engine);
	}

	public PostgresSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}

	/*
	 * Same as parent but replacing "outer join" with "full outer join"
	 */
	@Override
	protected void addJoin(String fromCol, String thisComparator, String toCol, String comparator) {
		// get the parts of the join
		List<String[]> relConPropList = getRelationshipConceptProperties(fromCol, toCol);
		for (String[] relConProp : relConPropList) {
			String sourceTable = relConProp[0];
			String sourceColumn = relConProp[1];
			String targetTable = relConProp[2];
			String targetColumn = relConProp[3];

			String compName = thisComparator.replace(".", " ");
			SqlJoinStruct jStruct = new SqlJoinStruct();
			// POSTGRES sql syntax requires the 'full' in outer join
			compName = compName.trim();
			if (compName.equals("outer join")) {
				compName = "full outer join";
			}
			jStruct.setJoinType(compName);
			// add source
			jStruct.setSourceTable(sourceTable);
			jStruct.setSourceTableAlias(getAlias(sourceTable));
			jStruct.setSourceCol(sourceColumn);
			// add target
			jStruct.setTargetTable(targetTable);
			jStruct.setTargetTableAlias(getAlias(targetTable));
			jStruct.setTargetCol(targetColumn);
			// set the comparator
			jStruct.setComparator(comparator);

			joinStructList.addJoin(jStruct);
		}
	}
}
