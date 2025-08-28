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
package prerna.reactor.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCurrentUserReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(GetCurrentUserReactor.class);

  @Override
  public NounMetadata execute() {
    Map<String, Object> returnMap = new TreeMap<String, Object>();
    User user = this.insight.getUser();
    if (user != null) {
      String userEpoch = user.getUserEpoch();
      for (AuthProvider provider : user.getLogins()) {
        String providerName = provider.name();
        AccessToken token = user.getAccessToken(provider);

        // add basic user details we capture
        Map<String, Object> providerMap = new TreeMap<>();
        providerMap.put("id", token.getId() == null ? "null" : token.getId());
        providerMap.put("name", token.getName() == null ? "null" : token.getName());
        providerMap.put("username", token.getUsername() == null ? "null" : token.getUsername());
        providerMap.put("email", token.getEmail() == null ? "null" : token.getEmail());
        providerMap.put(
            "lastPwdReset",
            token.getLastPasswordReset() == null ? "null" : token.getLastPasswordReset());
        providerMap.put("lastLogin", token.getLastLogin() == null ? "null" : token.getLastLogin());

        // add san info
        Map<String, String> san = token.getSAN();
        providerMap.put("san", san);

        // add group info
        Map<String, Object> groupMap = new HashMap<>();
        String groupType = token.getUserGroupType();
        Collection<String> groups = token.getUserGroups();
        groupMap.put("groupType", groupType);
        groupMap.put("groups", groups);
        providerMap.put("groupInfo", groupMap);

        // add user epoch into the login map
        providerMap.put("userEpoch", userEpoch);

        // add the entire map
        returnMap.put(providerName, providerMap);
      }
      Boolean isAdmin = SecurityAdminUtils.userIsAdmin(this.insight.getUser());
      returnMap.put("isAdmin", String.valueOf(isAdmin));
    } else {
      returnMap.put("No User", "User is not logged in");
    }
    NounMetadata noun =
        new NounMetadata(returnMap, PixelDataType.MAP, PixelOperationType.USER_INFO);
    return noun;
  }
}
