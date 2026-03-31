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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetLlmFeedbackCountReactor extends AbstractReactor {

    public AdminGetLlmFeedbackCountReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.START_DATE.getKey(),
                ReactorKeysEnum.END_DATE.getKey(),
                ReactorKeysEnum.PROJECT.getKey(),
                "userId",
                ReactorKeysEnum.ENGINE.getKey()
        };
    }

    @Override
    public NounMetadata execute() {
        User user = this.insight.getUser();
        SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
        if (adminUtils == null) {
            throw new IllegalArgumentException("User must be an admin to perform this function");
        }
        organizeKeys();

        String startDate = this.keyValue.get(this.keysToGet[0]);
        String endDate = this.keyValue.get(this.keysToGet[1]);
        String projectId = this.keyValue.get(this.keysToGet[2]);
        String userId = this.keyValue.get(this.keysToGet[3]);
        String engineId = this.keyValue.get(this.keysToGet[4]);

        List<Map<String, Object>> feedbackCounts = ModelInferenceLogsUtils.getFeedbackCountForAdmin(
                startDate, endDate, projectId, userId, engineId);

        return new NounMetadata(feedbackCounts, PixelDataType.FORMATTED_DATA_SET);
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals("userId")) {
            return "The user id to filter feedback counts by";
        }
        return super.getDescriptionForKey(key);
    }
}
