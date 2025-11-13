package prerna.auth.utils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.ReadOnlyAccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;

import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnitTestSecurityAuthUtils {

    static List<String> clearSecurityDB(RDBMSNativeEngine securityDb, List<String> tables) throws SQLException {
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

        return tables;
    }

    static void dumpQuery(String query, RDBMSNativeEngine securityDb) {
        try {
            doDumpQuery(query, securityDb);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void doDumpQuery(String query, RDBMSNativeEngine securityDb) throws SQLException {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = securityDb.getConnection();
            assertTrue(connection.getMetaData().getURL().contains("junit"));
            ps = connection.prepareStatement(query);
            ps.execute();
            rs = ps.getResultSet();
            printResults(rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            assert connection != null;
            connection.close();
        }
    }

    static void dumpTable(String tableName, RDBMSNativeEngine securityDb) {
        try {
            doDumpTable(tableName, securityDb);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void doDumpTable(String tableName, RDBMSNativeEngine securityDb) throws SQLException {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = securityDb.getConnection();
            assertTrue(connection.getMetaData().getURL().contains("junit"));
            ps = connection.prepareStatement(
                    "SELECT * FROM " + tableName);
            ps.execute();
            rs = ps.getResultSet();
            printResults(rs);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            assert connection != null;
            connection.close();
        }
    }

    private static void printResults(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();
        List<String[]> rows = new ArrayList<>();
        String[] headers = new String[cols];
        int[] widths = new int[cols];

        for (int i  = 1; i <= cols; i++) {
            headers[i - 1] = rsmd.getColumnName(i);
            widths[i - 1] = headers[i - 1].length();
        }

        while (rs.next()) {
            String[] row = new String[cols];
            for (int i = 1; i <= cols; i++) {
                String val = rs.getString(i);
                if (val == null) {
                    val = "NULL";
                }
                row[i - 1] = val;
                widths[i - 1] = Math.max(widths[i - 1], val.length());
            }
            rows.add(row);
        }

        StringBuilder border = new StringBuilder("+");
        for (int w : widths) {
            border.append(filler(w + 2, '-')).append("+");
        }
        System.out.println(border);
        System.out.print("|");
        for (int i = 0; i < cols; i++) {
            System.out.printf(" %-" + widths[i] + "s |", headers[i]);
        }
        System.out.println();
        System.out.println(border);

        for (String[] row : rows) {
            System.out.print("|");
            for (int i = 0; i < cols; i++) {
                System.out.printf(" %-" + widths[i] + "s |", row[i]);
            }
            System.out.println();
        }
        System.out.println(border);
    }

    static String filler(int count, char filler) {
        char[] arr = new char[count];
        Arrays.fill(arr, filler);
        return new String(arr);
    }

    static AccessToken createAccessToken(String prefix) {
        AccessToken at = new AccessToken();
        at.setProvider(AuthProvider.NATIVE);
        at.setName(prefix + "name");
        at.setId(prefix + "id");
        at.setUsername(prefix + "id");
        at.setEmail(prefix + "@test.com");
        return at;
    }

    static User createUser(String prefix, boolean admin) {
        User user = new User();
        user.setPrimaryLogin(AuthProvider.NATIVE);

        AccessToken at = createAccessToken(prefix);

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

    static User createAdminAddedUser(String prefix, boolean admin) {
        User user = new User();
        user.setPrimaryLogin(AuthProvider.NATIVE);

        AccessToken at = new AccessToken();
        at.setProvider(AuthProvider.NATIVE);
        at.setName("ADMIN_ADDED_USER");
        at.setId(prefix + "@test.com");
        at.setUsername("ADMIN_ADDED_USER");
        at.setEmail(prefix + "@test.com");

        user.setAccessToken(at);

        assertTrue(SecurityUpdateUtils.registerUser(
                prefix + "@test.com",
                "ADMIN_ADDED_USER",
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

    static void createProject(String id, String name, User user) {
        String userId = user.getPrimaryLoginToken().getId();
        SecurityProjectUtils.addProject(id, name, "APP", null, false, null, false, user);
        SecurityProjectUtils.addProjectOwner(user, id, userId);
    }

    static void createEngine(String id, String name, User user) {
        String userId = user.getPrimaryLoginToken().getId();
        SecurityEngineUtils.addEngine(id, name, IEngine.CATALOG_TYPE.DATABASE, null, null, false, user);
        SecurityEngineUtils.addEngineOwner(id, userId);
    }

    static void addPermissionsToUserForProject(User user, String pid, String uid, String permission) throws IllegalAccessException {
        String endDate = ZonedDateTime.now().plusDays(2).toString();
        List<Map<String, String>> permissions = List.of(Map.of("userid", uid, "permission", permission));
        SecurityProjectUtils.addProjectUserPermissions(user, pid, permissions, endDate);
    }

    static void createGroup(User user, String groupId, String groupType) {
        try {
            AdminSecurityGroupUtils.getInstance(user).addGroup(user, groupId, groupType, "short description");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void addUserToGroup(User user, String groupId, String userId, String userType) throws Exception {
        String endDate = ZonedDateTime.now().plusDays(2).toString();
        AdminSecurityGroupUtils.getInstance(user).addUserToGroup(user, groupId, userId, userType, endDate);
    }

    static void addUserTokenToGroup(User user, String groupid, String groupType) {
        ReadOnlyAccessToken at = (ReadOnlyAccessToken) user.getAccessToken(AuthProvider.NATIVE);
        AccessToken newAt = AccessToken.copyToken(at);
        Collection<String> existingUserGroups = newAt.getUserGroups();
        existingUserGroups.add(groupid);
        newAt.setUserGroups(new HashSet<>(existingUserGroups));
        newAt.setUserGroupType(groupType);
        user.setAccessToken(newAt);
    }

}
