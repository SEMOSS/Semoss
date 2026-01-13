package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Unit tests for {@link SessionReactor}.
 *
 * 
 */
public class SessionReactorUnitTests {

	/**
	 * When there is no existing shared {@link BrowserContext} on the user, the
	 * reactor must create a new context, store it on the user, create a session,
	 * and return the generated session id as a CONST_STRING.
	 */
	@Test
	public void createsNewContextWhenNoneExistsAndReturnsSessionId() {

		SessionReactor reactor = new SessionReactor();

		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		when(insight.getUser()).thenReturn(user);
		when(user.getSharedPlaywrightContext()).thenReturn(null);


		reactor.setInsight(insight);
		
		Browser browser = mock(Browser.class);
		BrowserContext context = mock(BrowserContext.class);
		Page page = mock(Page.class);

		try (MockedStatic<PlaywrightBrowserProvider> browserProviderMock = Mockito
				.mockStatic(PlaywrightBrowserProvider.class)) {
			browserProviderMock.when(PlaywrightBrowserProvider::getBrowser).thenReturn(browser);
			when(browser.newContext(any(Browser.NewContextOptions.class))).thenReturn(context);
			when(context.newPage()).thenReturn(page);

			NounMetadata result = reactor.execute();

			// 1. is context created
			verify(browser, times(1)).newContext(any(Browser.NewContextOptions.class));
			verify(context, times(1)).setDefaultTimeout(60_000);
			verify(context, times(1)).setDefaultNavigationTimeout(60_000);

			// 2. is context stored on user and page created
			verify(user, times(1)).setSharedPlaywrightContext(context);
			verify(context, times(1)).newPage();

			// 3. is session registered on user with id
			verify(user, times(1)).setPlaywrightSession(anyString(), Mockito.any(PlaywrightSession.class));

			// 4. is id returned as string
			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			String id = (String) result.getValue();
			assertFalse(id == null || id.isEmpty(), "Session id should be non-null and non-empty");
		}
	}

	/**
	 * When there is already a shared {@link BrowserContext}, the reactor should
	 * reuse it and must not create a new context via the provider.
	 */
	@Test
	public void reusesExistingContextwhenPresent() {
		
		SessionReactor reactor = new SessionReactor();
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		when(insight.getUser()).thenReturn(user);

		BrowserContext existingContext = mock(BrowserContext.class);
		Page page = mock(Page.class);
		when(user.getSharedPlaywrightContext()).thenReturn(existingContext);
		when(existingContext.newPage()).thenReturn(page);

		reactor.setInsight(insight);

		Browser browser = mock(Browser.class);

		try (MockedStatic<PlaywrightBrowserProvider> browserProviderMock = Mockito
				.mockStatic(PlaywrightBrowserProvider.class)) {
			browserProviderMock.when(PlaywrightBrowserProvider::getBrowser).thenReturn(browser);

			NounMetadata result = reactor.execute();

			verifyNoInteractions(browser);
			browserProviderMock.verify(PlaywrightBrowserProvider::getBrowser, times(1));

			verify(existingContext, times(1)).newPage();
			verify(user, times(1)).setPlaywrightSession(anyString(), Mockito.any(PlaywrightSession.class));

			assertEquals(PixelDataType.CONST_STRING, result.getNounType());
			String id = (String) result.getValue();
			assertFalse(id == null || id.isEmpty(), "Session id should be non-null and non-empty");

		}
	}
}

