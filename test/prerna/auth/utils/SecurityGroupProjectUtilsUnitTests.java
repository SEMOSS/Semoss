package prerna.auth.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.ReadOnlyAccessToken;
import prerna.auth.User;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;

import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SecurityGroupProjectUtilsUnitTests extends AbstractSecurityUtilsUnitTests {

    private RDBMSNativeEngine securityDb;

    private List<String> tables = new ArrayList<>();

    @BeforeEach
    void setup() {
        securityDb = AbstractSecurityUtils.securityDb;
        assertTrue(securityDb.getOwlFilePath().contains("junit"));
        assertNotNull(this.securityDb);
    }

    @AfterEach
    void cleanup() throws SQLException {
        assertTrue(securityDb.getOwlFilePath().contains("junit"));
        // clear test database inside of temp directory
        // quicker than deleting and recreating
        Statement statement = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = securityDb.getConnection();
            assertTrue(connection.getMetaData().getURL().contains("junit"));

            if (tables != null) {
                ps = connection.prepareStatement(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
                ps.execute();
                rs = ps.getResultSet();
                List<String> al = new ArrayList<>();
                while (rs.next()) {
                    al.add(rs.getString(1));
                }

                al.remove("PERMISSION");
                tables = al;
            }

            statement = connection.createStatement();
            for (String x : tables) {
                statement.addBatch("DELETE FROM " + x);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            assert statement != null;
            statement.close();
            connection.close();
        }
    }

    User createUser(String prefix, boolean admin) {
        User user = new User();
        user.setPrimaryLogin(AuthProvider.NATIVE);

        AccessToken at = new AccessToken();
        at.setProvider(AuthProvider.NATIVE);
        at.setName(prefix + "name");
        at.setId(prefix + "id");
        at.setUsername(prefix + "id");
        at.setEmail(prefix + "@test.com");

        user.setAccessToken(at);

        assertTrue(SecurityUpdateUtils.registerUser(
                prefix + "id",
                prefix + "name",
                prefix + "@test.com",
                "Test123!",
                AuthProvider.NATIVE.getLabel(),
                "5555555555",
                "001",
                "US",
                admin,
                false,
                false,
                null,
                null,
                null,
                null
        ));

        return user;
    }

    void createProject(String id, String name, User user) {
        String userId = user.getPrimaryLoginToken().getId();
        SecurityProjectUtils.addProject(id, name, "APP", null, false, null, false, user);
        SecurityProjectUtils.addProjectOwner(user, id, userId);

    }

    void addPermissionsToUserForProject(User user, String pid, String uid, String permission) throws IllegalAccessException {
        String endDate = ZonedDateTime.now().plusDays(2).toString();
        List<Map<String, String>> permissions = List.of(Map.of("userid", uid, "permission", permission));
       SecurityProjectUtils.addProjectUserPermissions(user, pid, permissions, endDate);
    }

    void createGroup(User user, String groupId, String groupType) {
        try {
            AdminSecurityGroupUtils.getInstance(user).addGroup(user, groupId, groupType, "short description");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void addUserToGroup(User user, String groupId, String userId, String userType) throws Exception {
        String endDate = ZonedDateTime.now().plusDays(2).toString();
        AdminSecurityGroupUtils.getInstance(user).addUserToGroup(user, groupId, userId, userType, endDate);
    }

    void addUserTokenToGroup(User user, String groupid, String groupType) {
        ReadOnlyAccessToken at = (ReadOnlyAccessToken) user.getAccessToken(AuthProvider.NATIVE);
        AccessToken newAt = AccessToken.copyToken(at);
        Collection<String> existingUserGroups = newAt.getUserGroups();
        existingUserGroups.add(groupid);
        newAt.setUserGroups(new HashSet<>(existingUserGroups));
        newAt.setUserGroupType(groupType);
        user.setAccessToken(newAt);
    }

    @Test
    void testUserGroupCanViewProjectWhileUserNotInGroup() {
        // create test user
        User user = createUser("admin", true);

        // create project as user
        createProject("pid1", "pname1", user);

        // check to see if user can view project
        assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));
    }


    @Test
    public void testUserGroupCanViewProjectUserGroupNoPermission() {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // check to see if user can view project
        assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));
    }

    //@TarameterizedTest
    @ValueSource(strings = {"OWNER", "EDIT", "READ_ONLY"})
    public void testUserGroupCanViewProjectUserGroupHasPermission(String permissionType) throws IllegalAccessException {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // end date within reason
        String endDate = ZonedDateTime.now().plusDays(2).toString();

        // assign permsission
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", permissionType, endDate);

        // check to see if user can view project
        assertTrue(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));
    }

    @Test
    public void testUserGroupCanViewProjectUserGroupHasPermissionButExpired() throws IllegalAccessException {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // end date within reason
        String endDate = ZonedDateTime.now().minusDays(2).toString();

        // assign permsission
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "1", endDate);

        // check to see if user can view project
        assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));

        // make sure that expired group permission was removed
        assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("pid1", "groupId1", "CUSTOM"));
    }

    @Test
    void testUserGroupCanEditProjectWhileUserNotInGroup() {
        // create test user
        User user = createUser("admin", true);

        // create project as user
        createProject("pid1", "pname1", user);

        // check to see if user can view project
        assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
    }


    @Test
    public void testUserGroupCanEditProjectUserGroupNoPermission() {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // check to see if user can edit project
        assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"OWNER", "EDIT", "READ_ONLY"})
    public void testUserGroupCanEditProjectUserGroupByPermissionType(String permissionType) throws IllegalAccessException {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // end date within reason
        String endDate = ZonedDateTime.now().plusDays(2).toString();

        // assign permsission
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", permissionType, endDate);

        // check to see if user can edit project
        if (permissionType.equalsIgnoreCase("READ_ONLY")) {
            assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
        } else {
            assertTrue(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
        }
    }

    @Test
    public void testUserGroupCanEditProjectUserGroupHasPermissionButExpired() throws IllegalAccessException {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // end date within reason
        String endDate = ZonedDateTime.now().minusDays(2).toString();

        // assign permsission
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "1", endDate);

        // check to see if user can edit project
        assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));

        // make sure that expired group permission was removed
        assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("pid1", "groupId1", "CUSTOM"));
    }

    @Test
    void testUserGroupIsOwnerGroupNotOwner() {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // check to see if user can view project
        assertFalse(SecurityGroupProjectUtils.userGroupIsOwner(user, "pid1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"OWNER", "EDIT", "READ_ONLY"})
    public void testUserGroupIsOnwerByPermissionType(String permissionType) throws IllegalAccessException {
        // create test user
        User user = createUser("admin", true);

        // create Group
        createGroup(user, "groupId1", "CUSTOM");

        // Set user token to group
        addUserTokenToGroup(user, "groupId1", "CUSTOM");

        // create project as user
        createProject("pid1", "pname1", user);

        // end date within reason
        String endDate = ZonedDateTime.now().plusDays(2).toString();

        // assign permsission
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", permissionType, endDate);

        // check to see if user can edit project
        if (permissionType.equalsIgnoreCase("OWNER")) {
            assertTrue(SecurityGroupProjectUtils.userGroupIsOwner(user, "pid1"));
        } else {
            assertFalse(SecurityGroupProjectUtils.userGroupIsOwner(user, "pid1"));
        }
    }

    @Test
    void testGetBestProjectPermissionGroupBetter() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        addPermissionsToUserForProject(user, "pid1", user2Id, "READ_ONLY");

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

        Integer permission = SecurityGroupProjectUtils.getBestProjectPermission(user2, "pid1");
        assertEquals(2, permission);
    }

    @Test
    void testGetBestProjectPermissionPersonalBetter() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate);

        Integer permission = SecurityGroupProjectUtils.getBestProjectPermission(user2, "pid1");
        assertEquals(2, permission);
    }


    @Test
    void testAddProjectGroupPermission_UserCannotEditProject() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add user to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        // try to giver user2 permissions as user2. Not allowed
        IllegalAccessException e = assertThrows(IllegalAccessException.class, () ->
                SecurityGroupProjectUtils.addProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate));
        assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
    }

    @Test
    void testAddProjectGroupPermission_GroupHasNoPermission() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate);

        // try to giver user2 permissions as user2. Not allowed
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate));
        assertEquals("This group already has access to this project. Please edit the existing permission level.", e.getMessage());
    }

    @Test
    void testGetGroupProjectPermission() throws IllegalAccessException {
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate);

        assertEquals(1, SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1").intValue());
    }

    @Test
    void testGetGroupProjectPermission_DoesNotExist() {
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1"));
    }

    @Test
    void testEditProjectGroupPermission_UserCannotEditProject() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add user to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        // try to giver user2 permissions as user2. Not allowed
        IllegalAccessException e = assertThrows(IllegalAccessException.class, () ->
                SecurityGroupProjectUtils.editProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate));
        assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
    }

    @Test
    void testEditProjectGroupPermission_GroupHasNoPermission() {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        // try to giver user2 permissions as user2. Not allowed
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SecurityGroupProjectUtils.editProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate));
        assertEquals("Attempting to modify group project permission for a group who does not currently have access to the project",
                e.getMessage());
    }

    @Test
    void testEditProjectGroupPermission_NotHighEnoughPermissions() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add user to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");


        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

        // try to giver user2 permissions as user2. Not allowed
        IllegalAccessException e = assertThrows(IllegalAccessException.class, () ->
                SecurityGroupProjectUtils.editProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "OWNER", endDate));
        assertEquals("Cannot give owner level access to this project since you are not currently an owner.", e.getMessage());
    }

    @Test
    void testEditProjectGroupPermission() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add user to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");


        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

        // set to read only
        SecurityGroupProjectUtils.editProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate);

        // verify read only
        Integer perm = SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1");
        assertEquals(3, perm);
    }

    @Test
    void testRemoveProjectGroupPermission_UserCannotEditProject() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add user to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        // try to giver user2 permissions as user2. Not allowed
        IllegalAccessException e = assertThrows(IllegalAccessException.class, () ->
                SecurityGroupProjectUtils.removeProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1"));
        assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
    }

    @Test
    void testRemoveProjectGroupPermission_GroupHasNoPermission() {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        String endDate = ZonedDateTime.now().plusDays(2).toString();
        // try to giver user2 permissions as user2. Not allowed
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SecurityGroupProjectUtils.removeProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1"));
        assertEquals("Attempting to modify group permission for a user who does not currently have access to the project",
                e.getMessage());
    }


    @Test
    void testRemoveProjectGroupPermission() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // create second user and add user to group
        User user2 = createUser("notadmin", false);
        addUserTokenToGroup(user2, "groupId1", "CUSTOM");
        String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
        String user2Type = user2.getPrimaryLogin().getLabel();
        addUserToGroup(user, "groupId1", user2Id, user2Type);

        addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");


        String endDate = ZonedDateTime.now().plusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

        // set to read only
        SecurityGroupProjectUtils.removeProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1");

        // verify permissions removed
        assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1"));
    }


    /**
     * remove Expired Project Group Permissions Tests
     */
    @Test
    void testExpiredRemoveProjectGroupPermission_NoPermission() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // set to read only
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SecurityGroupProjectUtils.removeExpiredProjectGroupPermission("groupId1", "CUSTOM", "pid1"));

        assertEquals(e.getMessage(), "Attempting to modify group permission for a user who does not currently have access to the project");
    }

    @Test
    void testExpiredRemoveProjectGroupPermission() throws Exception {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        createProject("pid1", "pname1", user);

        // minus days to go back 2 days
        String endDate = ZonedDateTime.now().minusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

        // set to read only
        SecurityGroupProjectUtils.removeExpiredProjectGroupPermission("groupId1", "CUSTOM", "pid1");

        // verify permissions removed
        assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1"));
    }


    @Test
    void testGetAllUserGroupProjects() throws IllegalAccessException {
        // create user, group, and project
        User user = createUser("admin", true);
        createGroup(user, "groupId1", "CUSTOM");
        createGroup(user, "groupId2", "CUSTOM");
        addUserTokenToGroup(user, "groupId1", "CUSTOM");
        addUserTokenToGroup(user, "groupId2", "CUSTOM");
        createProject("pid1", "pname1", user);
        createProject("pid2", "pname2", user);

        // minus days to go back 2 days
        String endDate = ZonedDateTime.now().minusDays(2).toString();
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);
        SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId2", "CUSTOM", "pid2", "EDIT", endDate);

        List<String> projectIds = SecurityGroupProjectUtils.getAllUserGroupProjects(user);
        assertTrue(projectIds.contains("pid1"));
        assertTrue(projectIds.contains("pid2"));
    }


}
