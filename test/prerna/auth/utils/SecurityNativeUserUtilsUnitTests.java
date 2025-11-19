package prerna.auth.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SecurityNativeUserUtilsUnitTests extends AbstractSecurityUtilsUnitTests {

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
        tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
        // This is here to clean up after a specific test
        DIHelper.getInstance().putProperty(Constants.MAX_USER_LIMIT, "1000");
    }

    ///
    /// AddNativeUser
    ///
    @Test
    void testAddNativeUser_Successful() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");

        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertEquals("testid", SecurityNativeUserUtils.getUserId("testid"));
    }

    @Test
    void testAddNativeUser_UserWasAddedByAdmin() {
        User user = UnitTestSecurityAuthUtils.doCreateUser("test", "ADMIN_ADDED_USER", "test", "test@test.com", true);
        AccessToken newAt = AccessToken.copyToken(user.getAccessToken(AuthProvider.NATIVE));
        newAt.setEmail("newEmail@test.com");
        newAt.setUsername("newId");

        assertTrue(SecurityNativeUserUtils.addNativeUser(newAt, "Test123!"));

        assertEquals("test", SecurityNativeUserUtils.getUserId("newId"));
        assertEquals("newEmail@test.com", SecurityNativeUserUtils.getUserEmail("newId"));
    }

    @Test
    void testAddNativeUser_OverLimitOfUsers() {
        DIHelper.getInstance().putProperty(Constants.MAX_USER_LIMIT, "1");
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        SecurityNativeUserUtils.addNativeUser(newUser, "Test123!");
        AccessToken newUser2 = UnitTestSecurityAuthUtils.createAccessToken("test");

        SemossPixelException e = assertThrows(SemossPixelException.class,
                () -> SecurityNativeUserUtils.addNativeUser(newUser2, "Test123!"));

        assertEquals("User limit exceeded the max value of 1", e.getMessage());
    }

    ///
    /// storeUserPasswords
    ///
    @Test
    void testStoreUserPasswords_Successful() throws Exception {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        SecurityNativeUserUtils.addNativeUser(newUser, "Test123!");

        Timestamp ts = Timestamp.valueOf(LocalDateTime.now());
        SecurityNativeUserUtils.storeUserPassword(newUser.getId(), newUser.getProvider().toString(), "hashed", "salt",
                ts);
    }

    @Test
    void testStoreUserPasswords_DeleteOldPasswords() throws Exception {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        SecurityNativeUserUtils.addNativeUser(newUser, "Test123!");

        // default password reset is 10. Add native stores a password. The first 9 iterations adds passwords
        // the 10th iteration should delete the oldest password. No good way to inspect database to ensure this happens.
        // but testing to make sure no error is thrown for deletion query
        for (int i = 0; i < 10; i++) {
            Timestamp ts = Timestamp.valueOf(LocalDateTime.now());
            SecurityNativeUserUtils.storeUserPassword(newUser.getId(), newUser.getProvider().toString(), "hashed", "salt",
                    ts);
        }
    }

    ///
    /// valid information
    ///
    @Test
    void testValidInformation_Successful() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        newUser.setPhone("5555555555");
        SecurityNativeUserUtils.validInformation(newUser, "Test123!", true);
    }

    @Test
    void testValidInformation_PhoneInvalid() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        newUser.setPhone("not a number");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> SecurityNativeUserUtils.validInformation(newUser, "Test123!", true));
        assertEquals("Phone number not a number contains invalid characters. ", e.getMessage());
    }

    @Test
    void testValidInformation_passwordInvalid() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> SecurityNativeUserUtils.validInformation(newUser, "bad", true));
        assertEquals("Password must be at least 8 characters in length." +
                "\nPassword must have atleast one uppercase character." +
                "\nPassword must have atleast one special character among [!,@,#,$,%,^,&,*]", e.getMessage());
    }

    @Test
    void testValidInformation_emailInvalid() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        newUser.setEmail("not a email");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> SecurityNativeUserUtils.validInformation(newUser, "Test123!", true));
        assertEquals("not a email is not a valid email address. ", e.getMessage());
    }

    @Test
    void testValidInformation_usernameInvalid() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        newUser.setUsername("");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> SecurityNativeUserUtils.validInformation(newUser, "Test123!", true));
        assertEquals("Username cannot be empty. ", e.getMessage());
    }


    ///
    /// log in
    ///

    @Test
    void testLogin_successful() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertTrue(SecurityNativeUserUtils.logIn("testid", "Test123!"));
    }

    @Test
    void testLogin_failed() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        // wrong password
        assertFalse(SecurityNativeUserUtils.logIn("testid", "wrong"));
        // wrong username
        assertFalse(SecurityNativeUserUtils.logIn("wrong", "Test123!"));
        // wrong both
        assertFalse(SecurityNativeUserUtils.logIn("wrong", "wrong"));
    }

    ///
    /// GetUsernameById
    ///

    @Test
    void testGetUsernameByuserId_Successful() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertEquals("testname",  SecurityNativeUserUtils.getUsernameByUserId(newUser.getId()));
    }

    @Test
    void testGetUsernameByuserId_Failed() {
        assertNull(SecurityNativeUserUtils.getUsernameByUserId("wrong"));
    }

    ///
    /// GetUserId
    ///

    @Test
    void testGetUserId_Successful() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertEquals("testid",  SecurityNativeUserUtils.getUserId(newUser.getUsername()));
    }

    @Test
    void testGetUserId_Failed() {
        assertNull(SecurityNativeUserUtils.getUserId("wrong"));
    }

    ///
    /// getUserEmail
    ///

    @Test
    void testGetUserEmail_Successful() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertEquals("test@test.com",  SecurityNativeUserUtils.getUserEmail(newUser.getUsername()));
    }

    @Test
    void testGetUserEmail_Failed() {
        assertNull(SecurityNativeUserUtils.getUserEmail("wrong"));
    }

    ///
    /// userEmailExists
    ///

    @Test
    void testUserEmailExists_exists() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));
        assertTrue(SecurityNativeUserUtils.userEmailExists(newUser.getEmail()));
    }

    @Test
    void testUserEmailExists_notExists() {
        assertFalse(SecurityNativeUserUtils.userEmailExists("wrong@test.com"));
    }

    ///
    /// getNameUser
    ///
    @Test
    void testGetNameUser_getName() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));
        assertEquals("testname", SecurityNativeUserUtils.getNameUser(newUser.getUsername()));
    }

    @Test
    void testGetNameUser_usernameNotFound() {
        assertNull(SecurityNativeUserUtils.getNameUser("wrong"));
    }

    ///
    /// isCurrentPassword
    ///

    @Test
    void testIsCurrentPassword_isCurrent() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));
        assertTrue(SecurityNativeUserUtils.isCurrentPassword(newUser.getId(), newUser.getProvider(), "Test123!"));
    }

    @Test
    void testIsCurrentPassword_notCurrent() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));
        assertFalse(SecurityNativeUserUtils.isCurrentPassword(newUser.getId(), newUser.getProvider(), "wrong"));
    }

    ///
    /// isPreviousPassword
    ///

    @Test
    void testIsPreviousPassword_isPrevious() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertTrue(SecurityNativeUserUtils.isPreviousPassword(newUser.getId(), newUser.getProvider(), "Test123!"));
    }

    @Test
    void testIsPreviousPassword_notPrevious() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));
        assertFalse(SecurityNativeUserUtils.isPreviousPassword(newUser.getId(), newUser.getProvider(), "wrong"));
    }

    ///
    /// performPasswordReset
    ///

    @Test
    void testPerformResetPassword_Successful() throws Exception {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        assertEquals("testid", SecurityNativeUserUtils.performResetPassword(newUser.getId(), "NewTest123!"));

        assertTrue(SecurityNativeUserUtils.logIn(newUser.getUsername(), "NewTest123!"));
    }

    @Test
    void testPerformResetPassword_notValid() {
        AccessToken newUser = UnitTestSecurityAuthUtils.createAccessToken("test");
        assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, "Test123!"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                SecurityNativeUserUtils.performResetPassword(newUser.getId(), "Bad55555"));
        assertEquals("Password must have atleast one special character among [!,@,#,$,%,^,&,*]", e.getMessage());

        // old still works, because new was invalid
        assertTrue(SecurityNativeUserUtils.logIn(newUser.getUsername(), "Test123!"));

        // new does not work since not valid
        assertFalse(SecurityNativeUserUtils.logIn(newUser.getUsername(), "Bad55555"));
    }
}
