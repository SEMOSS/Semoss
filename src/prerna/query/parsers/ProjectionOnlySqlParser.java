/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.query.parsers;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectionOnlySqlParser {

	public List<String> projections = new ArrayList<String>();

	public void processQuery(String query) throws Exception {
		// parse the sql
		Statement stmt = CCJSqlParserUtil.parse(query);
		Select select = ((Select) stmt);
		PlainSelect sb = (PlainSelect) select;

		List<SelectItem<?>> selects = sb.getSelectItems();
		for (int selectIndex = 0; selectIndex < selects.size(); selectIndex++) {
			SelectItem<?> si = selects.get(selectIndex);
			Alias seiAlias = si.getAlias();
			if (seiAlias == null) {
				projections.add(si.toString());
			} else {
				String aliasName = seiAlias.getName().trim();
				if (aliasName.startsWith("\"") || aliasName.startsWith("'")) {
					aliasName = aliasName.substring(1); // remove the first quote
				}
				if (aliasName.endsWith("\"") || aliasName.endsWith("'")) {
					aliasName = aliasName.substring(0, aliasName.length() - 1); // remove the end quote
				}
				projections.add(aliasName);
			}
		}
	}

	public List<String> getProjections() {
		return projections;
	}

}
