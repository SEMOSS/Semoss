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
package prerna.reactor.collaboration;

import java.util.HashMap;

import prerna.collaboration.BrainSync;
import prerna.collaboration.CollaborationJobUtils;
import prerna.collaboration.WorkThreadInsights;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainGetJob(); or BrainGetJob(kind=["import"]);
public class BrainGetJobReactor extends AbstractCollaborationReactor {

	private static final String KIND = "kind";
	private static final String JOB_ID = "jobId";
	private static final String MODE = "mode";

	public BrainGetJobReactor() {
		this.keysToGet = new String[] { KIND, JOB_ID, MODE };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		String jobId = getString(JOB_ID);
		var job = jobId == null ? CollaborationJobUtils.latest(user, getString(KIND), getString(MODE))
				: CollaborationJobUtils.get(user, jobId);
		var result = job == null ? new HashMap<String, Object>() : new HashMap<>(job);
		// a sync's summaries keep landing after it finishes; the page reads again until none are left
		if (BrainSync.KIND.equals(getString(KIND))) {
			result.put("insightsPending", WorkThreadInsights.pendingCount(user));
		}
		return mapResult(result);
	}

	@Override
	public String getReactorDescription() {
		return "Read an owner-scoped background job by exact ID or the newest import/classify job, optionally filtered by sort/topics mode: status, progress, counts and error; with kind sync also insightsPending, the thread summaries still running";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (KIND.equals(key)) {
			return "Job kind, for example import; omit for the newest of any kind";
		}
		if (JOB_ID.equals(key)) {
			return "Exact job ID belonging to the signed-in owner; takes precedence over kind and mode";
		}
		if (MODE.equals(key)) {
			return "With kind classify: sort or topics; omit for the newest job of either mode";
		}
		return super.getDescriptionForKey(key);
	}
}
