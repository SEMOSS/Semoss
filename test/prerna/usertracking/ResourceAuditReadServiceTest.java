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
package prerna.usertracking;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

class ResourceAuditReadServiceTest {
    JdbcTestDatabase db;
    User user;
    AccessToken token;
    MockedStatic<SecurityAdminUtils> admins;
    MockedStatic<AdminSecurityGroupUtils> groups;
    MockedStatic<Utility> utility;
    MockedStatic<UserAuditTrailUtils> audit;

    @BeforeEach
    void setup() throws Exception {
        db = new JdbcTestDatabase();
        db.registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
        admins = mockStatic(SecurityAdminUtils.class);
        groups = mockStatic(AdminSecurityGroupUtils.class);
        utility = mockStatic(Utility.class);
        audit = mockStatic(UserAuditTrailUtils.class);
        user = mock(User.class);
        token = mock(AccessToken.class);
        when(user.getLogins()).thenReturn(new ArrayList<>(List.of(AuthProvider.NATIVE)));
        when(user.getAccessToken(AuthProvider.NATIVE)).thenReturn(token);
        when(token.getId()).thenReturn("owner");
        when(token.getUserGroupType()).thenReturn("NATIVE");
        when(token.getUserGroups()).thenReturn(List.of());
        db.execute("CREATE TABLE ENGINE (ENGINEID VARCHAR)", "INSERT INTO ENGINE VALUES ('a'),('b')",
                "CREATE TABLE PROJECT (PROJECTID VARCHAR)", "INSERT INTO PROJECT VALUES ('p')",
                "CREATE TABLE ENGINEPERMISSION (ENGINEID VARCHAR,USERID VARCHAR,PERMISSION INT,ENDDATE TIMESTAMP)",
                "CREATE TABLE PROJECTPERMISSION (PROJECTID VARCHAR,USERID VARCHAR,PERMISSION INT,ENDDATE TIMESTAMP)",
                "CREATE TABLE GROUPENGINEPERMISSION (ENGINEID VARCHAR,ID VARCHAR,TYPE VARCHAR,PERMISSION INT,ENDDATE TIMESTAMP)",
                "CREATE TABLE GROUPPROJECTPERMISSION (PROJECTID VARCHAR,ID VARCHAR,TYPE VARCHAR,PERMISSION INT,ENDDATE TIMESTAMP)",
                "INSERT INTO ENGINEPERMISSION VALUES ('a','owner',1,NULL)");
        List<String> fields = List.of("EVENT_ID", "EVENT_TYPE", "ACTION", "STATUS", "CATEGORY", "SEVERITY",
                "ACTOR_USER_ID", "ACTOR_USER_TYPE", "ACTOR_USER_NAME", "SUBJECT_USER_ID", "SUBJECT_USER_TYPE",
                "SUBJECT_USER_NAME", "TARGET_TYPE", "TARGET_ID", "TARGET_NAME", "OLD_VALUE", "NEW_VALUE",
                "ENGINE_ID", "PROJECT_ID", "SESSION_ID_HASH", "IP_ADDR", "DETAILS", "ERROR_MESSAGE");
        db.execute("CREATE TABLE USER_AUDIT_EVENTS (EVENT_TIME TIMESTAMP," + String.join(",", fields.stream().map(f -> f + " VARCHAR").toList()) + ")");
    }

    @AfterEach
    void close() throws Exception {
        audit.close(); utility.close(); groups.close(); admins.close(); db.close();
    }

    void event(String eventId, String type, String targetType, String target, String engine, String project) throws Exception {
        try (var statement = db.connection.prepareStatement("INSERT INTO USER_AUDIT_EVENTS (EVENT_ID,EVENT_TIME,EVENT_TYPE,TARGET_TYPE,TARGET_ID,ENGINE_ID,PROJECT_ID,STATUS,OLD_VALUE,NEW_VALUE,IP_ADDR,DETAILS) VALUES (?,TIMESTAMP '2026-10-01 12:00:00',?,?,?,?,?,'SUCCESS','{\"permission\":\"READ_ONLY\",\"secret\":\"hidden\"}','{\"privateMetadata\":\"hidden\"}','127.0.0.1','secret')")) {
            statement.setString(1,eventId); statement.setString(2,type); statement.setString(3,targetType);
            statement.setString(4,target); statement.setString(5,engine); statement.setString(6,project); statement.executeUpdate();
        }
    }

    @Test
    void functionAndStorageLifecycleEventsAreVisibleOnlyForTheirEngine() throws Exception {
        event("function", "FUNCTION_CREATE", "FUNCTION", "a", "a", null);
        event("storage", "STORAGE_CREATE", "STORAGE", "b", "b", null);
        var functionRows = (List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()).get("events");
        assertEquals(1, functionRows.size());
        assertEquals("FUNCTION_CREATE", ((Map<?, ?>) functionRows.get(0)).get("EVENT_TYPE"));
        db.execute("INSERT INTO ENGINEPERMISSION VALUES ('b','owner',1,NULL)");
        var storageRows = (List<?>) ResourceAuditReadService.read(user, "ENGINE", "b", Map.of()).get("events");
        assertEquals(1, storageRows.size());
        assertEquals("STORAGE_CREATE", ((Map<?, ?>) storageRows.get(0)).get("EVENT_TYPE"));
    }

    @Test
    void isolatesCanonicalResourceAndRemovesPrivateFields() throws Exception {
        event("1","PERMISSION_UPDATE","ENGINE","a","a",null);
        event("2","PERMISSION_UPDATE","ENGINE","b","b",null);
        event("3","LOGIN","ENGINE","a","a",null);
        event("4","API_KEY_CREATE","ENGINE","a","a",null);
        event("5","PERMISSION_UPDATE","INSIGHT","private","a",null);
        var result = ResourceAuditReadService.read(user,"ENGINE","a",Map.of());
        var rows = (List<?>) result.get("events");
        assertEquals(1,rows.size());
        var row = (Map<?,?>) rows.get(0);
        assertEquals("READ_ONLY",row.get("OLD_VALUE"));
        assertNull(row.get("NEW_VALUE"));
        assertFalse(row.containsKey("IP_ADDR"));
        assertNull(row.get("DETAILS"));
        assertFalse(row.containsKey("SESSION_ID_HASH"));
    }

    @Test
    void onlyPermissionIdentityDetailsAreProjected() throws Exception {
        event("1","PERMISSION_ADD","ENGINE","a","a",null);
        db.execute("UPDATE USER_AUDIT_EVENTS SET DETAILS='{\"groupId\":\"team\",\"groupType\":\"CUSTOM\",\"token\":\"secret\",\"requestId\":\"internal\"}'");
        var row = (Map<?,?>)((List<?>)ResourceAuditReadService.read(user,"ENGINE","a",Map.of()).get("events")).get(0);
        assertEquals("{\"groupId\":\"team\",\"groupType\":\"CUSTOM\"}",row.get("DETAILS"));
    }

    @Test
    void paginationAndSortAreStable() throws Exception {
        event("1","ENGINE_CREATE","ENGINE","a","a",null);
        event("2","ENGINE_CREATE","ENGINE","a","a",null);
        var first = ResourceAuditReadService.read(user,"ENGINE","a",Map.of("pageSize","1","direction","ASC"));
        assertEquals(true, first.get("hasNextPage"));
        assertEquals("1",((Map<?,?>)((List<?>)first.get("events")).get(0)).get("EVENT_ID"));
        var second = ResourceAuditReadService.read(user,"ENGINE","a",Map.of("pageSize","1","page","1","direction","ASC"));
        assertEquals(false,second.get("hasNextPage"));
        assertEquals("2",((Map<?,?>)((List<?>)second.get("events")).get(0)).get("EVENT_ID"));
    }

    @Test
    void rejectsExpiredAndEditorGrantsAndRechecksAfterRevocation() throws Exception {
        assertTrue(ResourceAuditReadService.isOwner(user,"ENGINE","a"));
        db.execute("UPDATE ENGINEPERMISSION SET ENDDATE=TIMESTAMP '2000-01-01 00:00:00'");
        assertFalse(ResourceAuditReadService.isOwner(user,"ENGINE","a"));
        db.execute("UPDATE ENGINEPERMISSION SET ENDDATE=NULL,PERMISSION=2");
        assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of()));
        assertFalse(ResourceAuditReadService.isOwner(user,"ENGINE","b"));
    }

    @Test
    void severitySortingUsesRiskOrderNotAlphabeticalOrder() throws Exception {
        event("1","ENGINE_CREATE","ENGINE","a","a",null);
        event("2","ENGINE_CREATE","ENGINE","a","a",null);
        event("3","ENGINE_CREATE","ENGINE","a","a",null);
        db.execute("UPDATE USER_AUDIT_EVENTS SET SEVERITY='MEDIUM' WHERE EVENT_ID='1'",
                "UPDATE USER_AUDIT_EVENTS SET SEVERITY='HIGH' WHERE EVENT_ID='2'",
                "UPDATE USER_AUDIT_EVENTS SET SEVERITY='LOW' WHERE EVENT_ID='3'");
        var rows = (List<?>)ResourceAuditReadService.read(user,"ENGINE","a",Map.of("sort","SEVERITY","direction","DESC")).get("events");
        assertEquals("HIGH",((Map<?,?>)rows.get(0)).get("SEVERITY"));
        assertEquals("MEDIUM",((Map<?,?>)rows.get(1)).get("SEVERITY"));
        assertEquals("LOW",((Map<?,?>)rows.get(2)).get("SEVERITY"));
    }

    @Test
    void supportsActiveGroupsButNotExpiredGroups() throws Exception {
        when(token.getUserGroups()).thenReturn(List.of("owners"));
        db.execute("INSERT INTO GROUPENGINEPERMISSION VALUES ('b','owners','NATIVE',1,NULL)");
        assertTrue(ResourceAuditReadService.isOwner(user,"ENGINE","b"));
        db.execute("UPDATE GROUPENGINEPERMISSION SET ENDDATE=TIMESTAMP '2000-01-01 00:00:00'");
        assertFalse(ResourceAuditReadService.isOwner(user,"ENGINE","b"));
        groups.when(() -> AdminSecurityGroupUtils.getUserCustomGroups(token)).thenReturn(List.of("custom"));
        db.execute("INSERT INTO GROUPENGINEPERMISSION VALUES ('b','custom','CUSTOM',1,NULL)");
        assertTrue(ResourceAuditReadService.isOwner(user,"ENGINE","b"));
    }

    @Test
    void projectOwnershipDoesNotGrantEngineOrInsightHistory() throws Exception {
        db.execute("INSERT INTO PROJECTPERMISSION VALUES ('p','owner',1,NULL)");
        event("1","PROJECT_CREATE","PROJECT","p",null,"p");
        event("2","PERMISSION_ADD","INSIGHT","child",null,"p");
        event("3","ENGINE_CREATE","ENGINE","b","b","p");
        event("4","SKILL_CREATE","SKILL","p",null,"p");
        event("5","SKILL_UPDATE","SKILL","child",null,"p");
        assertEquals(2,((List<?>)ResourceAuditReadService.read(user,"PROJECT","p",Map.of()).get("events")).size());
        assertFalse(ResourceAuditReadService.isOwner(user,"ENGINE","b"));
    }

    @Test
    void filtersCannotBroadenScopeAndDatesAreInclusive() throws Exception {
        event("1","ENGINE_CREATE","ENGINE","a","a",null);
        assertEquals(1,((List<?>)ResourceAuditReadService.read(user,"ENGINE","a",Map.of("from","2026-10-01","to","2026-10-01")).get("events")).size());
        assertTrue(((List<?>)ResourceAuditReadService.read(user,"ENGINE","a",Map.of("status","FAILURE")).get("events")).isEmpty());
        assertTrue(((List<?>)ResourceAuditReadService.read(user,"ENGINE","a",Map.of("eventType","' OR 1=1 --")).get("events")).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of("engineId","b")));
        assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of("sort","EVENT_TIME; DROP TABLE ENGINE")));
        assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of("pageSize","101")));
        assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of("from","2026-02-30")));
        assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of("from","2026-10-03","to","2026-10-01")));
    }

    @Test
    void disabledTrackingAndAnonymousFailClosed() throws Exception {
        utility.when(Utility::isUserTrackingDisabled).thenReturn(true);
        assertThrows(IllegalStateException.class, () -> ResourceAuditReadService.read(user,"ENGINE","a",Map.of()));
        when(user.isAnonymous()).thenReturn(true);
        assertFalse(ResourceAuditReadService.isOwner(user,"ENGINE","a"));
    }

    @Test
    void deletedResourcesAreAdminOnly() throws Exception {
        db.execute("DELETE FROM ENGINE WHERE ENGINEID='a'");
        assertFalse(ResourceAuditReadService.isOwner(user,"ENGINE","a"));
        admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
        assertTrue(ResourceAuditReadService.isOwner(user,"ENGINE","a"));
    }
}
