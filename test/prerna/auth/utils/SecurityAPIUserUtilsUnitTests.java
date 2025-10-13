package prerna.auth.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.util.SocialPropertiesUtil;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class SecurityAPIUserUtilsUnitTests extends AbstractSecurityUtilsUnitTests {

    private AutoCloseable closeable;

    @Mock
    private SocialPropertiesUtil socialPropertiesUtil;

    static String id = "test123";
    static String email = "test123@test.com";
    static String type = "API_USER";

    @BeforeAll
    static void setUp() {
        // add user to security DB
        String name = "Test User";
        String password = "password123";
        String phone = "5551234567";
        String phoneextension = "001";
        String countrycode = "US";
        boolean admin = true;
        boolean publisher = false;
        boolean exporter = false;
        String modelUsageRestriction = null;
        String modelUsageFrequency = null;
        Integer modelMaxTokens = null;
        Double modelMaxResponseTime = null;

        boolean success = SecurityUpdateUtils.registerUser(id, name, email, password, type, phone, phoneextension,
                countrycode, admin, publisher, exporter, modelUsageRestriction, modelUsageFrequency, modelMaxTokens,
                modelMaxResponseTime);

        assertTrue(success, "Insertion of new user should be successful");
    }

    @BeforeEach
    void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void teardown() throws Exception {
        closeable.close();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "", " ", "\t", "\n", "true"})
    void testGetApplicationAPIUserTokenCheck(String token) {
        try (MockedStatic<SocialPropertiesUtil> sputils = Mockito.mockStatic(SocialPropertiesUtil.class)) {
            sputils.when(SocialPropertiesUtil::getInstance).thenReturn(socialPropertiesUtil);
            when(socialPropertiesUtil.getProperty("api_user_token_check")).thenReturn(token);

            assertTrue(SecurityAPIUserUtils.getApplicationAPIUserTokenCheck());
        }
    }

    @Test
    void testGetApplicationAPIUserTokenCheckFalse() {
        try (MockedStatic<SocialPropertiesUtil> sputils = Mockito.mockStatic(SocialPropertiesUtil.class)) {
            sputils.when(SocialPropertiesUtil::getInstance).thenReturn(socialPropertiesUtil);
            when(socialPropertiesUtil.getProperty("api_user_token_check")).thenReturn("false");

            assertFalse(SecurityAPIUserUtils.getApplicationAPIUserTokenCheck());
        }
    }


    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "", " ", "\t", "\n", "true"})
    void testGetApplicationAPIRequireDynamicToken(String token) {
        try (MockedStatic<SocialPropertiesUtil> sputils = Mockito.mockStatic(SocialPropertiesUtil.class)) {
            sputils.when(SocialPropertiesUtil::getInstance).thenReturn(socialPropertiesUtil);
            when(socialPropertiesUtil.getProperty("api_user_require_dynamic_token")).thenReturn(token);

            assertTrue(SecurityAPIUserUtils.getApplicationRequireDynamicToken());
        }
    }

    @Test
    void testGetApplicationAPIRequireDynamicTokenFalse() {
        try (MockedStatic<SocialPropertiesUtil> sputils = Mockito.mockStatic(SocialPropertiesUtil.class)) {
            sputils.when(SocialPropertiesUtil::getInstance).thenReturn(socialPropertiesUtil);
            when(socialPropertiesUtil.getProperty("api_user_require_dynamic_token")).thenReturn("false");

            assertFalse(SecurityAPIUserUtils.getApplicationRequireDynamicToken());
        }
    }

    @Test
    void testCreateApiUserAndValidCredentials() {
        Map<String, String> details = SecurityAPIUserUtils.createAPIUser("user1");
        assertNotNull(details.get("clientId"));
        assertNotNull(details.get("secretKey"));

        assertFalse(SecurityAPIUserUtils.validCredentials(details.get("clientId"), "wrong"));
        assertTrue(SecurityAPIUserUtils.validCredentials(details.get("clientId"), details.get("secretKey")));
    }


}
