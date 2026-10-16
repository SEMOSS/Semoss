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

import java.util.Map;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditChainStore;
import prerna.usertracking.AuditErrorCode;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditSeverity;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Checks the sequenced chain; legacy events and external trust anchors are
 * separate.
 */
public class AdminVerifyAuditTrailReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throwFunctionalityOnlyExposedForAdminsError();
		}
		if (Utility.isUserTrackingDisabled() || !SystemEngineRegistry.isUserTrackingDbLoaded()) {
			throw new IllegalStateException("Audit tracking is disabled or its database is unavailable");
		}
		try {
			long verified = AuditChainStore.verify(SystemEngineRegistry.getUserTrackingDb());
			var result = Map.of("verifiedEvents", verified, "scope", "V2_SEQUENCED_EVENTS");
			UserAuditTrailUtils.recordAuditAccess(this.insight.getUser(), AuditEventType.AUDIT_VERIFY, result);
			return new NounMetadata(result, PixelDataType.MAP);
		} catch (Exception e) {
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(AuditEventType.AUDIT_VERIFY)
					.actorUser(this.insight.getUser()).status(AuditStatus.FAILURE).severity(AuditSeverity.HIGH)
					.error(AuditErrorCode.AUDIT_INTEGRITY_CHECK_FAILED,
							"Audit verification could not certify the sequenced chain"));
			throw new IllegalStateException("Audit verification failed; investigate the chain and its trusted backup",
					e);
		}
	}
}
