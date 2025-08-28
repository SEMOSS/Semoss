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
package prerna.util.ldap;

import java.time.ZonedDateTime;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessToken;

public abstract class AbstractLdapAuthenticator implements ILdapAuthenticator {

  private static final Logger classLogger = LogManager.getLogger(AbstractLdapAuthenticator.class);

  @Override
  public DirContext createLdapContext(String providerUrl, String principalDN, String password)
      throws Exception {
    return LDAPConnectionHelper.createLdapContext(providerUrl, principalDN, password);
  }

  @Override
  public AccessToken generateAccessToken(
      Attributes attributes,
      String userDN,
      String attributeIdKey,
      String attributeNameKey,
      String attributeEmailKey,
      String attributeUserNameKey,
      String attributeLastPwdChangeKey,
      int requirePwdChangeAfterDays)
      throws Exception {
    return LDAPConnectionHelper.generateAccessToken(
        attributes,
        userDN,
        attributeIdKey,
        attributeNameKey,
        attributeEmailKey,
        attributeUserNameKey,
        attributeLastPwdChangeKey,
        requirePwdChangeAfterDays,
        false);
  }

  @Override
  public AccessToken generateAccessToken(
      Attributes attributes,
      String userDN,
      String attributeIdKey,
      String attributeNameKey,
      String attributeEmailKey,
      String attributeUserNameKey,
      String attributeLastPwdChangeKey,
      int requirePwdChangeAfterDays,
      boolean ignoreLastPwdChange)
      throws Exception {
    return LDAPConnectionHelper.generateAccessToken(
        attributes,
        userDN,
        attributeIdKey,
        attributeNameKey,
        attributeEmailKey,
        attributeUserNameKey,
        attributeLastPwdChangeKey,
        requirePwdChangeAfterDays,
        ignoreLastPwdChange);
  }

  @Override
  public ZonedDateTime getLastPwdChange(
      Attributes attributes, String attributeLastPwdChangeKey, int requirePwdChangeAfterDays)
      throws NamingException {
    return LDAPConnectionHelper.getLastPwdChange(
        attributes, attributeLastPwdChangeKey, requirePwdChangeAfterDays);
  }
}
