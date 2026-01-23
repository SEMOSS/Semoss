package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Playwright;

@Execution(ExecutionMode.SAME_THREAD)
class PlaywrightBrowserProviderUnitTests {

    @BeforeEach
    void init() throws Exception {
        resetProviderState();
    }

    @AfterEach
    void cleanup() throws Exception {
        resetProviderState();
    }

    @Test
    void getBrowser_initializesSingletonOnlyOnce() throws Exception {
        try (MockedStatic<Playwright> playwrightStatic = Mockito.mockStatic(Playwright.class)) {
            Playwright mockPlaywright = mock(Playwright.class);
            BrowserType mockBrowserType = mock(BrowserType.class);
            Browser mockBrowser = mock(Browser.class);

            playwrightStatic.when(Playwright::create).thenReturn(mockPlaywright);
            when(mockPlaywright.webkit()).thenReturn(mockBrowserType);
            when(mockBrowserType.launch(any(BrowserType.LaunchOptions.class))).thenReturn(mockBrowser);

            Browser first = PlaywrightBrowserProvider.getBrowser();
            Browser second = PlaywrightBrowserProvider.getBrowser();

            assertSame(mockBrowser, first);
            assertSame(first, second);
            playwrightStatic.verify(Playwright::create, times(1));
            verify(mockBrowserType, times(1)).launch(any(BrowserType.LaunchOptions.class));
        }
    }

    @Test
    void shutdown_closesBrowserAndPlaywright() throws Exception {
        Browser mockBrowser = mock(Browser.class);
        Playwright mockPlaywright = mock(Playwright.class);
        setStaticField("browser", mockBrowser);
        setStaticField("playwright", mockPlaywright);

        PlaywrightBrowserProvider.shutdown();

        verify(mockBrowser, times(1)).close();
        verify(mockPlaywright, times(1)).close();
    }

    private void resetProviderState() throws Exception {
        setStaticField("browser", null);
        setStaticField("playwright", null);
    }

    private void setStaticField(String name, Object value) throws Exception {
        Field field = PlaywrightBrowserProvider.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(null, value);
    }
}
