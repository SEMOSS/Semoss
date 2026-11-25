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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

class AppTestBrowserOptionsUnitTests {
    @Test void headlessRemainsTheDefaultAndCannotKeepAnInvisibleWindow() {
        assertTrue(AppTestBrowserOptions.DEFAULT.headless());
        assertFalse(AppTestBrowserOptions.DEFAULT.keepOpen());
        assertThrows(IllegalArgumentException.class, () -> new AppTestBrowserOptions(true, true));
        assertDoesNotThrow(() -> new AppTestBrowserOptions(false, true));
        var reactor = new RunAppTestsReactor().getReactorDefinition();
        assertEquals(Boolean.class, reactor.getKey("headless").getType());
        assertEquals(Boolean.class, reactor.getKey("keepOpen").getType());
    }

    @Test void inspectionEndsWhenTheWindowClosesAndReleasesThePrivateDatabase() {
        var page = mock(Page.class);
        var context = mock(BrowserContext.class);
        var database = mock(AppTestEnvironment.class);
        when(page.isClosed()).thenReturn(false, true);
        try (var inspection = new AppTestRunner.Inspection()) {
            inspection.page = page; inspection.context = context; inspection.environment = database;
            inspection.awaitClose(60_000);
            verify(page).waitForTimeout(anyDouble());
            verify(database, never()).close();
        }
        verify(context).close();
        verify(database).close();
    }

    @Test void inspectionTimeoutAndAlreadyClosedWindowsDoNotWaitOrLeak() {
        var page = mock(Page.class);
        try (var inspection = new AppTestRunner.Inspection()) {
            inspection.page = page;
            inspection.awaitClose(0);
            verify(page, never()).waitForTimeout(anyDouble());
            when(page.isClosed()).thenReturn(true);
            inspection.awaitClose(60_000);
            verify(page, never()).waitForTimeout(anyDouble());
        }
    }

    @Test void manualCloseErrorsDoNotExtendTheInspectionWaitAndDatabaseStillCloses() {
        var page = mock(Page.class);
        var context = mock(BrowserContext.class);
        var database = mock(AppTestEnvironment.class);
        doThrow(new RuntimeException("Window closed")).when(page).waitForTimeout(anyDouble());
        try (var inspection = new AppTestRunner.Inspection()) {
            inspection.page = page; inspection.context = context; inspection.environment = database;
            assertDoesNotThrow(() -> inspection.awaitClose(60_000));
        }
        verify(context).close(); verify(database).close();
    }

    @Test void persistedHistoryKeepsPresentationSettingsButNeverClaimsAWindowIsStillOpen() {
        var saved = AppTestHistory.compact(Map.of("runId", "run", "headless", false, "keepOpen", true,
                "browserOpen", true, "browserClosesAt", "temporary"));
        assertEquals(false, saved.get("headless")); assertEquals(true, saved.get("keepOpen"));
        assertFalse(saved.containsKey("browserOpen")); assertFalse(saved.containsKey("browserClosesAt"));
    }
}
