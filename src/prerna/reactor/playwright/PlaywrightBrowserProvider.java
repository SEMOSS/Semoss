package prerna.reactor.playwright;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Playwright;

final class PlaywrightBrowserProvider {

	private static volatile Playwright playwright;
	private static volatile Browser browser;

	private PlaywrightBrowserProvider() {

	}

	static Browser getBrowser() {
		Browser localBrowser = browser;
		if (localBrowser == null) {
			synchronized (PlaywrightBrowserProvider.class) {
				if (browser == null) {
					playwright = Playwright.create();
					browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true));
				}
				localBrowser = browser;
			}
		}
		return localBrowser;
	}

	static void shutdown() {
		synchronized (PlaywrightBrowserProvider.class) {
			try {
				if (browser != null) {
					browser.close();
				}
			} catch (Exception ignored) {
			} finally {
				browser = null;
				try {
					if (playwright != null) {
						playwright.close();
					}
				} catch (Exception ignored) {
				} finally {
					playwright = null;
				}
			}
		}
	}
}