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
package prerna.usertracking.reactors;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;

public class UserActivitySQLiteReactor extends AbstractQueryStructReactor {

	// UserActivity example
	// UserActivity ( ) | Import ( frame = [ CreateFrame ( frameType = [ GRID ] ,
	// override = [ true ] ) .as ( [ "FRAME961184" ] ) ] ) ;
	// Frame ( frame = [ FRAME961184 ] ) | UserActivity ( ) | AutoTaskOptions (
	// panel = [ "0" ] , layout = [ "Grid" ] ) | Collect ( 2000 ) ;

	// date format function example
	// Frame ( frame = [ FRAME961184 ] ) | Select ( DateFormat ( "%Y-%m-%d" ,
	// DATE_CREATED ) ) | CollectAll ( ) ;

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		this.qs.setEngineId("UserTrackingDatabase");
		this.qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

		SelectQueryStruct sQs = new SelectQueryStruct();
		// selectors
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("COUNT");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "USERID"));
		sQs.addSelector(fSelector);

		sQs.addSelector(new QueryColumnSelector("USER_TRACKING" + "__" + "CREATED_ON"));
//		// group by
		sQs.addGroupBy(new QueryColumnSelector("USER_TRACKING" + "__" + "CREATED_ON"));
		// order by
		sQs.addOrderBy("USER_TRACKING" + "__" + "CREATED_ON", "DESC");
		;
		this.qs.merge(sQs);
		return this.qs;
	}

	@Override
	public String getReactorDescription() {
		return "Builds a query struct returning user login counts by day from the SQLite user tracking database.";
	}
}
