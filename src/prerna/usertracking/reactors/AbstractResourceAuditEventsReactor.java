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

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditErrorCode;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.ResourceAuditReadService;
import prerna.usertracking.UserAuditTrailUtils;

public abstract class AbstractResourceAuditEventsReactor extends AbstractReactor {

	protected abstract String scope();

	protected AbstractResourceAuditEventsReactor() {
		this.keysToGet = new String[] { "id", "page", "pageSize", "sort", "direction", "eventType", "category",
				"status", "severity", "actorId", "subjectId", "from", "to" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, String> options = new HashMap<>(this.keyValue);
		String id = options.remove("id");
		try {
			return new NounMetadata(ResourceAuditReadService.read(this.insight.getUser(), scope(), id, options),
					PixelDataType.MAP);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (IllegalStateException e) {
			recordReadFailure(id);
			throw e;
		} catch (Exception e) {
			recordReadFailure(id);
			throw new IllegalStateException("Unable to read the resource audit trail.", e);
		}
	}

	private void recordReadFailure(String id) {
		UserAuditTrailUtils.recordEvent(
				new UserAuditTrailUtils.AuditEvent().eventType(AuditEventType.AUDIT_QUERY).status(AuditStatus.FAILURE)
						.actorUser(this.insight.getUser()).target(AuditTargetType.valueOf(scope()), id, null)
						.context(scope().equals("PROJECT") ? id : null, scope().equals("ENGINE") ? id : null, null,
								null)
						.error(AuditErrorCode.AUDIT_READ_FAILED, "Unable to read the resource audit trail."));
	}
}
