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
package prerna.reactor.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetUserMetakeyOptionsReactor extends AbstractInsightReactor {

  private static final String METAOPTIONS = "metaoptions";

  public SetUserMetakeyOptionsReactor() {
    this.keysToGet = new String[] {METAOPTIONS};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    boolean res = false;
    SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
    if (adminUtils == null) {
      throw new IllegalArgumentException("User is not an admin.");
    } else {
      organizeKeys();
      List<Map<String, Object>> metaoptions = getMetaOptions();
      if (metaoptions == null || metaoptions.isEmpty()) {
        throw new IllegalArgumentException("Must provide a set of metadata values to store.");
      }
      res = SecurityUserUtils.updateMetakeyOptions(metaoptions);
    }

    NounMetadata noun = new NounMetadata(res, PixelDataType.BOOLEAN);
    if (res) {
      noun.addAdditionalReturn(
          NounMetadata.getSuccessNounMessage(
              "Successfully updated the new metakey for metakey options"));
    } else {
      noun.addAdditionalReturn(
          NounMetadata.getErrorNounMessage(
              "Did not update metakey options. Please check your inputs and try again."));
    }
    return noun;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> getMetaOptions() {
    GenRowStruct mapGrs = this.store.getNoun(METAOPTIONS);
    if (mapGrs != null && !mapGrs.isEmpty()) {
      List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
      if (mapInputs != null && !mapInputs.isEmpty()) {
        List<Map<String, Object>> res = new ArrayList<>();
        for (int i = 0; i < mapInputs.size(); i++) {
          res.add((Map<String, Object>) mapInputs.get(i).getValue());
        }
        return res;
      }
    }
    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      List<Map<String, Object>> res = new ArrayList<>();
      for (int i = 0; i < mapInputs.size(); i++) {
        res.add((Map<String, Object>) mapInputs.get(i).getValue());
      }
      return res;
    }
    return null;
  }

  @Override
  public String getReactorDescription() {
    return "Define metadata options for users";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(METAOPTIONS)) {
      return "Array of maps like {'metakey': '...', 'single_multi': '...', 'display_order': #, 'display_options': '...', 'display_values': '...'} containing list of metadata options possible for users";
    }
    return super.getDescriptionForKey(key);
  }
}
