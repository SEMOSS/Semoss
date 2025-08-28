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
package prerna.reactor.insights;

import java.util.List;
import prerna.auth.User;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightFrameStructureReactor extends AbstractReactor {

	/*
	 * PAYLOAD MUST MATCH THAT OF {@link
	 * prerna.sablecc2.reactor.frame.GetFrameTableStructureReactor}
	 */

	public GetInsightFrameStructureReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);

		User user = this.insight.getUser();
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityInsightUtils.userCanViewInsight(user, projectId, rdbmsId)) {
			NounMetadata noun = new NounMetadata("User does not have access to this insight",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// get a list of frames info
		List<Object[]> insightFrames = SecurityInsightUtils.getInsightFrames(projectId, rdbmsId);

		return new NounMetadata(insightFrames, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}
