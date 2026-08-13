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
 * 	MERCHANTIBILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.usertracking.reactors;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class AdminUserAuditEventsReactor extends AbstractQueryStructReactor {

	private static final String TABLE = "USER_AUDIT_EVENTS";

	@Override
	public NounMetadata execute() {
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throwFunctionalityOnlyExposedForAdminsError();
		}
		if (Utility.isUserTrackingDisabled()) {
			throw new IllegalArgumentException("User tracking is disabled. Enable USER_TRACKING_ENABLED to query "
					+ TABLE + ".");
		}
		if (!SystemEngineRegistry.isUserTrackingDbLoaded()) {
			throw new IllegalStateException("User tracking database is not loaded. Confirm "
					+ Constants.USER_TRACKING_DB + ".smss exists and restart SEMOSS.");
		}
		return super.execute();
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		this.qs.setEngineId(Constants.USER_TRACKING_DB);
		this.qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

		SelectQueryStruct sQs = new SelectQueryStruct();
		addSelector(sQs, "EVENT_ID");
		addSelector(sQs, "EVENT_TIME");
		addSelector(sQs, "EVENT_TYPE");
		addSelector(sQs, "ACTION");
		addSelector(sQs, "STATUS");
		addSelector(sQs, "ACTOR_USER_ID");
		addSelector(sQs, "ACTOR_USER_TYPE");
		addSelector(sQs, "ACTOR_USER_NAME");
		addSelector(sQs, "SESSION_ID");
		addSelector(sQs, "REQUEST_ID");
		addSelector(sQs, "IP_ADDR");
		addSelector(sQs, "TARGET_TYPE");
		addSelector(sQs, "TARGET_ID");
		addSelector(sQs, "TARGET_NAME");
		addSelector(sQs, "PROJECT_ID");
		addSelector(sQs, "ENGINE_ID");
		addSelector(sQs, "INSIGHT_ID");
		addSelector(sQs, "ROOM_ID");
		addSelector(sQs, "OLD_VALUE");
		addSelector(sQs, "NEW_VALUE");
		addSelector(sQs, "DETAILS");
		addSelector(sQs, "ERROR_MESSAGE");
		sQs.addOrderBy(TABLE + "__EVENT_TIME", "DESC");

		this.qs.merge(sQs);
		return this.qs;
	}

	private static void addSelector(SelectQueryStruct qs, String column) {
		qs.addSelector(new QueryColumnSelector(TABLE + "__" + column));
	}

	@Override
	public String getReactorDescription() {
		return "Admin-only query struct for reading business/security audit events from the user tracking database.";
	}
}
