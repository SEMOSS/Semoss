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
package prerna.auth.utils;

import jakarta.mail.Session;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.SemossUnitTest;
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

public class UserRegistrationEmailServiceUnitTests extends SemossUnitTest {

    private static UserRegistrationEmailService instance;

    @BeforeAll
    static void setup() throws IOException {
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
