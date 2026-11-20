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
package prerna.auth.utils.reactors.admin;

import prerna.auth.utils.EnterpriseUsageUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Admin-only, searchable catalog choices for enterprise usage filters. */
public class AdminGetEnterpriseUsageFilterOptionsReactor extends AbstractReactor {

	public AdminGetEnterpriseUsageFilterOptionsReactor() {
		this.keysToGet = new String[] { "dimension", "search", "id", "limit", "offset" };
	}

	@Override
	public NounMetadata execute() {
		if (SecurityAdminUtils.getInstance(insight.getUser()) == null) {
			throw new IllegalArgumentException("User Must Be An Admin To Read Enterprise Filter Options");
		}
		organizeKeys();
		var dimension = EnterpriseUsageUtils.option(EnterpriseUsageUtils.FilterDimension.class,
				keyValue.get("dimension"));
		int limit = EnterpriseUsageUtils.boundedInteger(keyValue.get("limit"), 50, 1, 100);
		int offset = EnterpriseUsageUtils.boundedInteger(keyValue.get("offset"), 0, 0, Integer.MAX_VALUE - 101);
		return new NounMetadata(EnterpriseUsageUtils.filterOptions(dimension, keyValue.get("search"),
				keyValue.get("id"), limit, offset), PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Lists Registered User IDs, Names And Authentication Types, App IDs And Names, Or Engine IDs And Names For Enterprise Usage Filters. Supports Bound Search, Exact ID Resolution And Bounded Pagination.";
	}
}
