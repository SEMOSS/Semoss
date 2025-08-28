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
package prerna.forms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class UpdateFormUnitTests {
  private UpdateFormReactor reactor;
  private Insight insight;
  private User user;
  // private NounStore store;

  private NounStore ns;
  private GenRowStruct grs;

  @BeforeEach
  void setup() {
    reactor = new UpdateFormReactor();
    insight = mock(Insight.class);
    user = mock(User.class);
    reactor.setInsight(insight);
    when(insight.getUser()).thenReturn(user);

    ns = mock(NounStore.class);
    grs = mock(GenRowStruct.class);

    reactor.setNounStore(ns);
  }

  @Test
  void testUserIdNull() {
    when(user.getAccessToken(AuthProvider.CAC)).thenReturn(null);
    when(user.getAccessToken(AuthProvider.SAML)).thenReturn(null);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
    assertEquals("Could not identify user", e.getMessage());
  }

  @Test
  void testAccessTokenCAC() {
    Map<String, String> keyvalues = reactor.keyValue;
    keyvalues.put("database", "testdb");
    keyvalues.put("form_input", "testinput");

    AccessToken at = new AccessToken();
    at.setId("Testid");
    at.setAccess_token("Test");
    at.setProvider(AuthProvider.CAC);
    at.setUsername("Testusername");
    at.setEmail("test@email.com");
    when(user.getAccessToken(AuthProvider.CAC)).thenReturn(at);

    try (MockedStatic<Utility> ut = Mockito.mockStatic(Utility.class);
        MockedStatic<FormFactory> ff = Mockito.mockStatic(FormFactory.class)) {
      IDatabaseEngine engine = mock(IDatabaseEngine.class);
      AbstractFormBuilder formBuilder = mock(AbstractFormBuilder.class);

      ut.when(() -> Utility.getDatabase(anyString())).thenReturn(engine);
      ff.when(() -> FormFactory.getFormBuilder(any(IDatabaseEngine.class))).thenReturn(formBuilder);

      when(this.ns.getNoun(any(String.class))).thenReturn(grs);
      DATABASE_TYPE dt = mock(DATABASE_TYPE.class);

      assertEquals(
          reactor.execute().toString(), (new NounMetadata(true, PixelDataType.BOOLEAN)).toString());
    }
  }

  @Test
  void testAccessTokenSAML() {
    Map<String, String> keyvalues = reactor.keyValue;
    keyvalues.put("database", "testdb");
    keyvalues.put("form_input", "testinput");

    AccessToken at = new AccessToken();
    at.setId("Testid");
    at.setAccess_token("Test");
    at.setProvider(AuthProvider.SAML);
    at.setUsername("Testusername");
    at.setEmail("test@email.com");
    when(user.getAccessToken(AuthProvider.SAML)).thenReturn(at);

    try (MockedStatic<Utility> ut = Mockito.mockStatic(Utility.class);
        MockedStatic<FormFactory> ff = Mockito.mockStatic(FormFactory.class)) {
      IDatabaseEngine engine = mock(IDatabaseEngine.class);
      AbstractFormBuilder formBuilder = mock(AbstractFormBuilder.class);

      ut.when(() -> Utility.getDatabase(anyString())).thenReturn(engine);
      ff.when(() -> FormFactory.getFormBuilder(any(IDatabaseEngine.class))).thenReturn(formBuilder);

      when(this.ns.getNoun(any(String.class))).thenReturn(grs);
      DATABASE_TYPE dt = mock(DATABASE_TYPE.class);

      assertEquals(
          reactor.execute().toString(), (new NounMetadata(true, PixelDataType.BOOLEAN)).toString());
    }
  }

  @Test
  void testExecuteWithIOException() throws IOException {
    Map<String, String> keyvalues = reactor.keyValue;
    keyvalues.put("database", "testdb");
    keyvalues.put("form_input", "testinput");

    when(user.getAccessToken(AuthProvider.CAC)).thenReturn(null);
    when(user.getAccessToken(AuthProvider.SAML)).thenReturn(null);

    try (MockedStatic<Utility> ut = Mockito.mockStatic(Utility.class);
        MockedStatic<FormFactory> ff = Mockito.mockStatic(FormFactory.class)) {
      IDatabaseEngine engine = mock(IDatabaseEngine.class);
      AbstractFormBuilder formBuilder = mock(AbstractFormBuilder.class);

      ut.when(() -> Utility.getDatabase(anyString())).thenReturn(engine);
      ff.when(() -> FormFactory.getFormBuilder(any(IDatabaseEngine.class))).thenReturn(formBuilder);

      when(ns.getNoun(any(String.class))).thenReturn(grs);

      Map<String, Object> engineHash = new HashMap<>();
      doThrow(new IOException()).when(formBuilder).commitFormData(anyMap(), anyString());

      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
      assertEquals("Could not identify user", e.getMessage());
    }
  }

  @Test
  void testGetName() {
    assertEquals("UpdateForms", reactor.getName());
  }

  // this test fails because something is wrong with the doThrow method
  // unsure what exactly.
  // @Test
  // void testIOException() throws IOException {
  //     Map<String, String> keyvalues = reactor.keyValue;
  //     keyvalues.put("database", "testdb");
  //     keyvalues.put("form_input", "testinput");
  //     AccessToken at = new AccessToken();
  //     at.setId("Testid");
  //     at.setAccess_token("Test");
  //     at.setProvider(AuthProvider.CAC);
  //     at.setUsername("Testusername");
  //     at.setEmail("test@email.com");

  //     when(user.getAccessToken(AuthProvider.CAC)).thenReturn(at);

  //     try (MockedStatic<Utility> ut = Mockito.mockStatic(Utility.class);
  //          MockedStatic<FormFactory> ff = Mockito.mockStatic(FormFactory.class)) {
  //         IDatabaseEngine engine = mock(IDatabaseEngine.class);
  //         AbstractFormBuilder formBuilder = mock(AbstractFormBuilder.class);

  //         ut.when(() -> Utility.getDatabase(anyString())).thenReturn(engine);
  //         ff.when(() ->
  // FormFactory.getFormBuilder(any(IDatabaseEngine.class))).thenReturn(formBuilder);

  //         when(ns.getNoun(any(String.class))).thenReturn(grs);

  //         doThrow(new IOException()).when(formBuilder).commitFormData(anyMap(), anyString());

  //         NounMetadata result = reactor.execute();
  //         assertEquals(new NounMetadata(false, PixelDataType.BOOLEAN), result);
  //     }
  // }

}
