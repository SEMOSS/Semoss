package prerna.auth.utils;

import jakarta.mail.Session;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UserRegistrationEmailServiceUnitTests {

    private static UserRegistrationEmailService instance;

    @BeforeAll
    static void setup(@TempDir Path tempDir) throws IOException {
        Path template = tempDir.resolve("emailTemplates");
        Files.createDirectories(template);
        Path requestFile = template.resolve("passResetRequest.html");
        Files.createFile(requestFile);
        List<String> lines = new ArrayList<>();
        lines.add("test {{{REPLACE_LINK}}}");

        Files.write(requestFile, lines);

        Path successFile = template.resolve("passResetSuccess.html");
        Files.createFile(successFile);
        lines = new ArrayList<>();
        lines.add("test");
        Files.write(successFile, lines);

        try (MockedStatic<Utility> utils = Mockito.mockStatic(Utility.class)) {
            utils.when(Utility::getBaseFolder).thenReturn(tempDir.toAbsolutePath().toString());
            instance = UserRegistrationEmailService.getInstance();
            assertNotNull(instance);
        }
    }

    @Test
    void getInstance() throws NoSuchFieldException, IllegalAccessException {

        try (MockedStatic<Utility> utils = Mockito.mockStatic(Utility.class)) {
            Field f = instance.getClass().getDeclaredField("emailTemplatesFolder");
            f.setAccessible(true);
            String path = (String) f.get(instance);
            assertTrue(path.endsWith("emailTemplates/"));
            instance = UserRegistrationEmailService.getInstance();
            assertNotNull(instance);
            path = (String) f.get(instance);
            assertTrue(path.endsWith("emailTemplates/"));
            utils.verify(Utility::getBaseFolder, times(0));
        }

    }

    @Test
    void testSendPasswordResetRequestEmail() {

        try (MockedStatic<SocialPropertiesUtil> sputils = Mockito.mockStatic(SocialPropertiesUtil.class);
             MockedStatic<EmailUtility> emailUtils = Mockito.mockStatic(EmailUtility.class)) {

            UserRegistrationEmailService instance = UserRegistrationEmailService.getInstance();
            assertNotNull(instance);

            SocialPropertiesUtil socialProps = mock(SocialPropertiesUtil.class);
            sputils.when(SocialPropertiesUtil::getInstance).thenReturn(socialProps);

            Session session = mock(Session.class);
            when(socialProps.getEmailSession()).thenReturn(session);
            when(socialProps.getSmtpSender()).thenReturn("sender");

            emailUtils.when(() -> EmailUtility.sendEmail(eq(session), eq(new String[]{"rec"}), isNull(), isNull(),
                    eq("sender"), eq("SEMOSS Reset Password : Request"), anyString(), eq(true), isNull()))
                    .thenReturn(true);

            boolean success = instance.sendPasswordResetRequestEmail("rec", "custom", "");
            assertTrue(success);
            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            emailUtils.verify(() -> EmailUtility.sendEmail(eq(session), eq(new String[]{"rec"}), isNull(), isNull(),
                    eq("sender"), eq("SEMOSS Reset Password : Request"), captor.capture(), eq(true), isNull()), times(1));
            assertEquals("test custom", captor.getValue().trim());
        }
    }

    @Test
    void testSendPasswordResetSuccessEmail() {
        try (MockedStatic<SocialPropertiesUtil> sputils = Mockito.mockStatic(SocialPropertiesUtil.class);
             MockedStatic<EmailUtility> emailUtils = Mockito.mockStatic(EmailUtility.class)) {

            UserRegistrationEmailService instance = UserRegistrationEmailService.getInstance();
            assertNotNull(instance);

            SocialPropertiesUtil socialProps = mock(SocialPropertiesUtil.class);
            sputils.when(SocialPropertiesUtil::getInstance).thenReturn(socialProps);

            Session session = mock(Session.class);
            when(socialProps.getEmailSession()).thenReturn(session);
            when(socialProps.getSmtpSender()).thenReturn("sender");

            emailUtils.when(() -> EmailUtility.sendEmail(eq(session), eq(new String[]{"rec"}), isNull(), isNull(),
                            eq("sender"), eq("SEMOSS Reset Password : Success"), anyString(), eq(true), isNull()))
                    .thenReturn(true);

            boolean success = instance.sendPasswordResetSuccessEmail("rec", "");
            assertTrue(success);
            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
            emailUtils.verify(() -> EmailUtility.sendEmail(eq(session), eq(new String[]{"rec"}), isNull(), isNull(),
                    eq("sender"), eq("SEMOSS Reset Password : Success"), captor.capture(), eq(true), isNull()), times(1));
            assertEquals("test", captor.getValue().trim());
        }
    }

}
