/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.test;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.BrowserType.LaunchOptions;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.BrowserChannel;
import com.microsoft.playwright.options.Cookie;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PlaywrightTrace {

	public static void main(String[] args) {
		int count = 0;

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		try (Playwright playwright = Playwright.create()) {
			LaunchOptions lp = new LaunchOptions();
			lp.setChannel(BrowserChannel.CHROME);
			lp.setHeadless(false);
			Browser firefox = playwright.chromium().launch(new BrowserType.LaunchOptions().setChannel("msedge"));
			BrowserContext ctx = firefox.newContext();
			// BrowserType firefox = playwright.chromium();
			// BrowserType firefox = playwright.webkit();
			// Browser browser = firefox.launch();
			// BrowserContext ctx = browser.newContext();
			// ctx.addCookies(getCookies());
			// page = ctx.newPage();
			Page page = ctx.newPage();

			// page.navigate("https://www.expedia.com/");

			String baseUrl = "https://mail.google.com/";
			baseUrl = "https://www.kayak.com/";
			baseUrl = "https://deloittenet.deloitte.com";
			baseUrl = "https://dte.deloittenet.com";

			// baseUrl = "https://ecommerce-playground.lambdatest.io/";
			page.navigate(baseUrl);
			// Thread.sleep(2000);
			String data = null;
			System.err.println("Enter locator");
			do {
				Locator where = null;
				// br.readLine();
				/*
				 * practice where = page.getByRole(AriaRole.BUTTON, new
				 * Page.GetByRoleOptions().setName("Search"));
				 * System.err.println(where.getAttribute("class"));
				 */
				// where.get
				// data-stid="destination_form_field-menu-trigger"
				// where = page.getByRole(AriaRole.BUTTON, new
				// Page.GetByRoleOptions().setName("Where
				// to?"));

				// kayak
				/*
				 * 
				 * page.screenshot(new
				 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
				 * ".png"))); FileUtils.write(new File("c:/temp/kayak.html"), page.content());
				 * 
				 * page.locator("#main-search-form").getByRole(AriaRole.LIST).getByRole(AriaRole
				 * .BUTTON).click(); page.getByPlaceholder("From?").click();
				 * page.getByPlaceholder("From?").fill("WAS"); Thread.sleep(1000);
				 * FileUtils.write(new File("c:/temp/from.html"), page.content());
				 * page.screenshot(new
				 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
				 * ".png"))); page.getByLabel("Washington, D.C., District of").
				 * getByText("Washington, D.C., District of").click();
				 * page.getByPlaceholder("To?").click();
				 * page.getByPlaceholder("To?").fill("LAX");
				 * page.getByLabel("Los Angeles, California,").
				 * getByText("Los Angeles, California,").click();
				 * 
				 * page.screenshot(new
				 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
				 * ".png"))); page.getByLabel("Start date").click();
				 * //page.getByPlaceholder("From?").click();
				 * //page.getByPlaceholder("To?").click(); //Thread.sleep(1000);
				 * //page.getByRole(AriaRole.BUTTON, new
				 * Page.GetByRoleOptions().setName("Search")).click(); //page.screenshot(new
				 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
				 * ".png"))); Page page1 = page.waitForPopup(() -> {
				 * page.getByRole(AriaRole.BUTTON, new
				 * Page.GetByRoleOptions().setName("Search")).click(); }); page1.screenshot(new
				 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
				 * ".png")));
				 */

				// dte
				page.navigate(baseUrl);
				page.getByPlaceholder("Email, phone, or Skype").click();
				page.getByPlaceholder("Email, phone, or Skype").fill("pkapaleeswaran@deloitte.com");
				page.getByPlaceholder("Email, phone, or Skype").press("Enter");
				page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Next")).click();
				page.locator("#i0118").fill("monkey boy");
				page.locator("#i0118").press("Enter");
				page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Sign in")).click();
				page.getByLabel("Remember this browser").check();
				page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Continue")).click();
				page.locator("#carouselModal").getByLabel("Close").click();
				page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("23 Nov 2024 40.00")).click();
				page.getByLabel("OK, Got it").click();
				page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Close")).click();
				page.locator("panel").filter(new Locator.FilterOptions().setHasText("VA Virginia, VA 0.00 8.00 8."))
						.getByLabel("Expand Tax Jurisdiction").click();

				page.locator("#time-entry-1").getByLabel("Attendance Hours entry for Monday").click();
				page.locator("#time-entry-1").getByLabel("Attendance Hours entry for Monday").fill("18");
				page.locator("#time-entry-1").getByLabel("Attendance Hours entry for Monday").press("Tab");

				page.getByText("DOD00159-02-01-01-0110 SEMOSS Bus Analytics 2001 00")
						.getByLabel("Attendance Hours entry for Monday").click();

				// deloitte
				/*
				 * page.locator(".onetrust-pc-dark-filter").click();
				 * page.getByRole(AriaRole.BUTTON, new
				 * Page.GetByRoleOptions().setName("Accept and close")).click();
				 * System.err.println("Accepting the button"); page.getByRole(AriaRole.LINK, new
				 * Page.GetByRoleOptions().setName("Perspectives 2025 investment")).click();
				 * page.getByRole(AriaRole.LINK, new
				 * Page.GetByRoleOptions().setName("last year’s outlook")).click();
				 * page.getByLabel("For You", new
				 * Page.GetByLabelOptions().setExact(true)).click();
				 * page.getByRole(AriaRole.LINK, new
				 * Page.GetByRoleOptions().setName("Man and woman walking")).click();
				 * System.err.println("Priting screen.. "); page.screenshot(new
				 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
				 * ".png")));
				 */
				/*
				 * //String locator = "//*[@data-stid=\"destination_form_field-menu-trigger\"]";
				 * //String selector = "Where would you like to stay?"; //String selector2 =
				 * "//*[@id=\"lodging_search_form\"]/div/div/div[1]/div/div/div[1]/button";
				 * //selector =
				 * "pierce/#lodging_search_form > div > div > div:nth-of-type(1) button";
				 * //where = page.getByPlaceholder(selector); //where.fill("WAS");
				 * //where.click(); //where.press("Enter");
				 * 
				 * where.press("ArrowDown"); where.press("Enter"); where.press("Escape");
				 * where.click(); //page.locator(".farefinder-container").click();
				 * Thread.sleep(1000); //page.keyboard().
				 * 
				 * //ElementHandle handle = page.querySelector(selector); //handle.click();
				 * //Thread.sleep(1000); //handle = page.querySelector(selector2);
				 * //System.err.println("Found it.. " + where.getAttribute("class")); //where =
				 * page.locator(locator); Locator search = page.getByRole(AriaRole.BUTTON, new
				 * Page.GetByRoleOptions().setName("Find a hotel"));
				 * System.err.println("Visibility.. " + search.isVisible() +
				 * search.isEnabled()); //where.vi search.click(); //while (where.isVisible())
				 * // where.click();
				 *
				 */
				Thread.sleep(3000);
				// FileUtils.write(new File("c:/temp/popup.html"), page.content());

			} while ((data = br.readLine()) != null);

			// browser.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static List<Cookie> getCookies() {
		List<Cookie> retList = new ArrayList<Cookie>();
		Cookie c1 = new Cookie("ACCOUNT_CHOOSER",
				"AFx_qI4UZRWnMomilswGgklUgd_JNP5JqKvh1kouxDwCfvx8KibqsOxVjm3w_rd8tLrl5EIv8wcvEGYaIwpTLuEPNv8KPjG63SBTFtFbR1sIa5TzhGRh9_bEEq6SSpQDpQpQAs38R4HY");
		c1.setDomain("accounts.google.com");
		c1.setPath("/");
		String expiryDate = "2025-10-17T03:11:54.164Z";
		c1.setExpires(expires(expiryDate));

		Cookie c2 = new Cookie("AEC", "AVYB7cpIW7bExUEa73FoiH1rUK763hf-TNBVbfbTrfKVCLxpGykTa68LhQ");
		c2.setDomain(".google.com");
		c2.setPath("/");
		expiryDate = "2025-03-25T22:46:21.547Z";
		c2.setExpires(expires(expiryDate));

		Cookie c3 = new Cookie("APSID", "ce7_pnDbTYap4IkW/AlPJ9b6URhOXoPt5Y");
		c3.setDomain(".google.com");
		c3.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c3.setExpires(expires(expiryDate));

		Cookie c4 = new Cookie("HSID", "Au7lzneP7KUzrRs3r");
		c4.setDomain(".google.com");
		c4.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c4.setExpires(expires(expiryDate));

		Cookie c5 = new Cookie("LSID",
				"o.chat.google.com|o.chromewebstore.google.com|o.console.cloud.google.com|o.drive.google.com|o.gds.google.com|o.groups.google.com|o.lens.google.com|o.mail.google.com|o.meet.google.com|o.myaccount.google.com|o.play.google.com|s.blogger|s.youtube:g.a000pAj6D-Y3ZpzWCjD983QcsV_OfZS1imGpYn4q9vXUjuvDjGtECwVkLu5Tj1yMUx5Epm26CQACgYKAVkSARASFQHGX2MiaMvhWOFsL3DGv63wYh6KuBoVAUF8yKq3pFPaR5mSMcgGF7588unx0076");
		c5.setDomain("accounts.google.com");
		c5.setPath("/");
		expiryDate = "2025-12-08T02:41:38.739Z";
		c5.setExpires(expires(expiryDate));

		Cookie c6 = new Cookie("LSOLH",
				"_SVI_ENTKxa--tIkDGAUiP01BRURIZl9xc3ZMbWJiLXVHdTl2N0taTlNXVzh5bWxjM1p6aVp1Si0wZTFERS1xckh0cVdFaGdUaWNsbjhuRQ_:28837277:147e");
		c6.setDomain("accounts.google.com");
		c6.setPath("/");
		expiryDate = "2025-10-29T21:02:08.014Z";
		c6.setExpires(expires(expiryDate));

		Cookie c7 = new Cookie("NID",
				"518=1wtlfrW9Dxuwn8s-N9TpbJjLYusMa3NrwJ0P-XVNrwfhZ9cgiQh-TFXy9Sn_ZQmzAPkpbuHZiDkiUDVr1Sy81xlFKipf_-b2LKaWzyitOOtHhh4S8TPqOBnx86hlSftRE85yKUnkv1kkO1uKJJXYQtrTvAqITkWnFVGaUk7j1IiQCQ5xYCaqlKZTCaBsMATfAoHxvtMp1rLhWleCbyh6sjPYtWdIfsadx8xjlyJ08cek5P-3d8-Z4uOQQ6HEq2rUwK2wySYJ1wDGwxPicyPEEl6d8bd9BpDAPDsUoE1ZKj8LBATnvjQ_4B4Y0m0-iFzZvhicxMuq80yqV0L3REEmSVZHXeVV3XrV0vjrbebu0SMplAN7Ff-sqb-ac4iOAMq5lZgKGzzXvFq5CaZqi1I7qRw-Sz9tRYqmAxnDoO5CIxtzD27F3lvKvU2kU9g2J-_G6Z3go2vkk3q6gj1c1eunVn0WuSimQycxhlyF8r7Pzw8NCLDHC-WSE1Yb9B_TROSRT1ai-oHB2puC1rARFZ2rY6nllnPsUiKYD1LMyjyIdXvFEoGMceoFH07B0XFq2glYCvUfnjQ4Gy39YhyzNDzsirc3pDbmea-pjZCeW-x3sEFwaIbm8KSHuXJlTfaX9HAv98pnPdRM40PaDBQYOEcl2Qxphs-K4jXep7qXTKxaubX_Sibogt1KPmpRadyvM6hiYXS8Vz-lgkaGBMZQHqN7yydWcUtu5DdyOg-6FkF_xUs5fi5sVGun7vJ8em1w59LYPqO9QfkVG3GIvPUUTPyk_25YWzVWN8MTVMcuQU5tUSEHuoJMluSCADfhDFPRbA-e-TyWxWD68sfsMlluomigvckXTESGAQXs");
		c7.setDomain(".google.com");
		c7.setPath("/");
		expiryDate = "2025-05-08T00:29:10.152Z";
		c7.setExpires(expires(expiryDate));

		Cookie c8 = new Cookie("OGP", "-19041976:");
		c8.setDomain(".google.com");
		c8.setPath("/");
		expiryDate = "2024-11-14T03:04:41.000Z";
		c8.setExpires(expires(expiryDate));

		Cookie c9 = new Cookie("OGPC", "19041976-1:");
		c9.setDomain(".google.com");
		c9.setPath("/");
		expiryDate = "2024-11-14T02:57:53.000Z";
		c9.setExpires(expires(expiryDate));

		Cookie c10 = new Cookie("OTZ", "7774746_72_76_104100_72_446760");
		c10.setDomain("accounts.google.com");
		c10.setPath("/");
		expiryDate = "2024-11-12T03:06:13.000Z";
		c10.setExpires(expires(expiryDate));

		Cookie c11 = new Cookie("SAPISID", "0E3yl-PErXbT11n1/ANNWqgTppbOdOSBm4");
		c11.setDomain(".google.com");
		c11.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c11.setExpires(expires(expiryDate));

		Cookie c12 = new Cookie("SID",
				"g.a000pAj6D-FqJds98dNdDWTIW_tuBAJjU4_jHnqd9ddQZRBF_LHVKHIZi-kF6XxkF2ux4Fx6eAACgYKAewSAQASFQHGX2MiPrSdoNkUFoC0D0shHuZ3thoVAUF8yKoWx3bIW2CgCrZ3i1EMtFEa0076");
		c12.setDomain(".google.com");
		c12.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c12.setExpires(expires(expiryDate));

		Cookie c13 = new Cookie("SIDCC", "AKEyXzWjVHyvMKbuPGUC1e1QgvjlkgKTmVMCFFDrlQq90sNoldRkSWmoYwpmpMNfY1zmE0sps5M");
		c13.setDomain(".google.com");
		c13.setPath("/");
		expiryDate = "2025-11-06T02:10:12.984Z";
		c13.setExpires(expires(expiryDate));

		Cookie c14 = new Cookie("SSID", "AnpqtFamWBjmpU1_Y");
		c14.setDomain(".google.com");
		c14.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c14.setExpires(expires(expiryDate));

		Cookie c15 = new Cookie("__Host-1PLSID",
				"o.chat.google.com|o.chromewebstore.google.com|o.console.cloud.google.com|o.drive.google.com|o.gds.google.com|o.groups.google.com|o.lens.google.com|o.mail.google.com|o.meet.google.com|o.myaccount.google.com|o.play.google.com|s.blogger|s.youtube:g.a000pAj6D-Y3ZpzWCjD983QcsV_OfZS1imGpYn4q9vXUjuvDjGtEqnNMMIwnTlMjrs17OSfNZAACgYKASISARASFQHGX2Miv1pNm6iZwWNmcMFfUNlh4hoVAUF8yKp_AwD3ZSMSSv-zhAoPvZkl0076");
		c15.setDomain("accounts.google.com");
		c15.setPath("/");
		expiryDate = "2025-12-08T02:41:38.739Z";
		c15.setExpires(expires(expiryDate));

		Cookie c16 = new Cookie("__Host-3PLSID",
				"o.chat.google.com|o.chromewebstore.google.com|o.console.cloud.google.com|o.drive.google.com|o.gds.google.com|o.groups.google.com|o.lens.google.com|o.mail.google.com|o.meet.google.com|o.myaccount.google.com|o.play.google.com|s.blogger|s.youtube:g.a000pAj6D-Y3ZpzWCjD983QcsV_OfZS1imGpYn4q9vXUjuvDjGtE617x_d_yAJEQ_5jMl5vQuAACgYKAcYSARASFQHGX2MiQS2TrgusIVSgXivgwtgR9BoVAUF8yKrrlOzpAUqIuYCj3sTlye0p0076");
		c16.setDomain("accounts.google.com");
		c16.setPath("/");
		expiryDate = "2025-12-08T02:41:38.739Z";
		c16.setExpires(expires(expiryDate));

		Cookie c17 = new Cookie("__Host-GAPS",
				"1:J98HHrBFpJVZwP3fYQlgJGeFnqzzCw1rVChtsmD9gVuPjyrjOA2DAYTFssxEebFa0LLXsyPzlJABwQXM58O4TzVY4L4Kbw:0jK9xW1AHlAry3OY");
		c17.setDomain("accounts.google.com");
		c17.setPath("/");
		expiryDate = "2025-12-08T02:41:38.739Z";
		c17.setExpires(expires(expiryDate));

		Cookie c18 = new Cookie("__Secure-1PAPISID", "0E3yl-PErXbT11n1/ANNWqgTppbOdOSBm4");
		c18.setDomain(".google.com");
		c18.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c18.setExpires(expires(expiryDate));

		Cookie c19 = new Cookie("__Secure-1PSID",
				"g.a000pAj6D-FqJds98dNdDWTIW_tuBAJjU4_jHnqd9ddQZRBF_LHVV3VSQmr_JWUyWac2LSFgGgACgYKAbwSAQASFQHGX2Mikl1UjCnX9UwPcjNB3ybOExoVAUF8yKqakATQEB8Fk84km8Dv8mPf0076");
		c19.setDomain(".google.com");
		c19.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c19.setExpires(expires(expiryDate));

		Cookie c20 = new Cookie("__Secure-1PSIDCC",
				"AKEyXzVcoaRIiHqtPITrXM1T8yjFjKcSWaybUeEv3KTjNstsaDWIc4PdNnvWB2NsOwIbuhcEu0U");
		c20.setDomain(".google.com");
		c20.setPath("/");
		expiryDate = "2025-11-06T02:10:12.984Z";
		c20.setExpires(expires(expiryDate));

		Cookie c21 = new Cookie("__Secure-1PSIDTS",
				"sidts-CjIBQT4rXyVTpZ1rnYgPDt9SXsNhzQcfUNSA7L-nySsr4RYbETdr4SMUDnvj8yiI7LX-GhAA");
		c21.setDomain(".google.com");
		c21.setPath("/");
		expiryDate = "2025-11-06T02:10:12.984Z";
		c21.setExpires(expires(expiryDate));

		Cookie c22 = new Cookie("__Secure-3PAPISID", "0E3yl-PErXbT11n1/ANNWqgTppbOdOSBm4");
		c22.setDomain(".google.com");
		c22.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c22.setExpires(expires(expiryDate));

		Cookie c23 = new Cookie("__Secure-3PSID",
				"g.a000pAj6D-FqJds98dNdDWTIW_tuBAJjU4_jHnqd9ddQZRBF_LHV4IeoSUGmhpE8eMqkXNxfBwACgYKASMSAQASFQHGX2MiGfXCfb425AjJXfxClUHdpxoVAUF8yKqBPqi5iFBNuQhxLL6I1cel0076");
		c23.setDomain(".google.com");
		c23.setPath("/");
		expiryDate = "2025-11-16T19:37:29.748Z";
		c23.setExpires(expires(expiryDate));

		Cookie c24 = new Cookie("__Secure-3PSIDCC",
				"AKEyXzX7fQbszHP22ySj5DRao2o_SAD5Uqg4_SWT9jPhYrsI2Z8Rs5qQn8_HNYRdaP7tn9Vh7MY");
		c24.setDomain(".google.com");
		c24.setPath("/");
		expiryDate = "2025-11-06T02:10:12.984Z";
		c24.setExpires(expires(expiryDate));

		Cookie c25 = new Cookie("__Secure-3PSIDTS",
				"sidts-CjIBQT4rXyVTpZ1rnYgPDt9SXsNhzQcfUNSA7L-nySsr4RYbETdr4SMUDnvj8yiI7LX-GhAA");
		c25.setDomain(".google.com");
		c25.setPath("/");
		expiryDate = "2025-11-06T02:10:12.984Z";
		c25.setExpires(expires(expiryDate));

		Cookie c26 = new Cookie("__Secure-BUCKET", "CLIG");
		c26.setDomain(".google.com");
		c26.setPath("/");
		expiryDate = "2025-04-27T20:53:16.727Z";
		c26.setExpires(expires(expiryDate));

		retList.add(c1);
		retList.add(c2);
		retList.add(c3);
		retList.add(c4);
		retList.add(c5);
		retList.add(c6);
		retList.add(c7);
		retList.add(c8);
		retList.add(c9);
		retList.add(c10);
		retList.add(c11);
		retList.add(c12);
		retList.add(c13);
		retList.add(c14);
		/*
		 * retList.add(c15); retList.add(c16); retList.add(c17); retList.add(c18);
		 * retList.add(c19); retList.add(c20); retList.add(c21); retList.add(c22);
		 * retList.add(c23); retList.add(c24); retList.add(c25); retList.add(c26);
		 */

		return retList;
	}

	public static double expires(String inDate) {
		// 2025-05-08T00:29:10.152Z
		DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
		DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd-MM-yyy", Locale.ENGLISH);
		// "2018-04-10T04:00:00.000Z"
		LocalDateTime date = LocalDateTime.parse(inDate, inputFormatter);
		LocalDateTime now = LocalDateTime.now();
		// Duration duration = Duration.between(now, date);
		String formattedDate = outputFormatter.format(date);
		System.out.println(formattedDate); // prints 10-04-2018
		return ChronoUnit.SECONDS.between(now, date);
	}

	/*
	 * Deloitte Login page.navigate(
	 * "https://login.microsoftonline.com/36da45f1-dd2c-4d1f-af13-5abe46b99921/wsfed/?wa=wsignin1.0&wtrealm=urn%3adeloittenet%3asharepoint&wctx=https%3a%2f%2fdeloittenet.deloitte.com%2f_layouts%2f15%2fAuthenticate.aspx%3fSource%3d%252F&sso_reload=true"
	 * ); page.getByPlaceholder("Email, phone, or Skype").click();
	 * page.getByPlaceholder("Email, phone, or Skype").fill(
	 * "pkapaleeswaran@deloitte.com");
	 * page.getByPlaceholder("Email, phone, or Skype").press("Enter");
	 * page.getByRole(AriaRole.BUTTON, new
	 * Page.GetByRoleOptions().setName("Next")).click();
	 * page.locator("#i0118").fill("monkey boy"); // password
	 * page.getByRole(AriaRole.BUTTON, new
	 * Page.GetByRoleOptions().setName("Sign in")).click();
	 * 
	 * page.getByLabel("Remember this browser").check();
	 * page.getByRole(AriaRole.BUTTON, new
	 * Page.GetByRoleOptions().setName("Continue")).click();
	 * 
	 * page.getByPlaceholder("Search apps, people, links,").click();
	 * page.getByPlaceholder("Search apps, people, links,").fill("deloit");
	 * page.getByPlaceholder("Search apps, people, links,").click();
	 * page.getByPlaceholder("Search apps, people, links,").dblclick();
	 * page.getByPlaceholder("Search apps, people, links,").fill("dte"); Page page1
	 * = page.waitForPopup(() -> { page.getByRole(AriaRole.LINK, new
	 * Page.GetByRoleOptions().setName("DTE: Deloitte Time & Expense")).click(); });
	 * page1.locator("#carouselModal").getByLabel("Close").click();
	 * page1.getByRole(AriaRole.BUTTON, new
	 * Page.GetByRoleOptions().setName("Close")).click();
	 * page1.getByRole(AriaRole.LINK, new
	 * Page.GetByRoleOptions().setName("Nov 2024 0.00 0.00")).click();
	 * page1.getByLabel("OK, Got it").click(); page1.locator("panel").filter(new
	 * Locator.FilterOptions().setHasText("VA Virginia, VA 0.00 0.00 0.")).
	 * getByLabel("Expand Tax Jurisdiction").click();
	 * page1.locator("#time-entry-20005").
	 * getByLabel("Attendance Hours entry for Monday").click();
	 * page1.locator("#time-entry-20005").
	 * getByLabel("Attendance Hours entry for Monday").fill("8");
	 * page1.locator("#time-entry-20005").
	 * getByLabel("Attendance Hours entry for Monday").press("Tab");
	 * page1.locator("#time-entry-20005").
	 * getByLabel("Attendance Hours entry for Tuesday").fill("8");
	 * page1.locator("#time-entry-20005").
	 * getByLabel("Attendance Hours entry for Tuesday").press("Tab");
	 * page1.locator("#time-entry-20005").
	 * getByLabel("Attendance Hours entry for Wednesday").fill("8");
	 * page1.getByLabel("Save").click(); page1.getByRole(AriaRole.BUTTON, new
	 * Page.GetByRoleOptions().setName("Close")).click();
	 *
	 *
	 * dte
	 * 
	 * page.locator("#carouselModal").getByLabel("Close").click();
	 * page.getByRole(AriaRole.BUTTON, new
	 * Page.GetByRoleOptions().setName("Close")).click();
	 * page.getByRole(AriaRole.LINK, new
	 * Page.GetByRoleOptions().setName("23 Nov 2024 40.00")).click();
	 * page.getByLabel("OK, Got it").click(); page.locator("panel").filter(new
	 * Locator.FilterOptions().setHasText("VA Virginia, VA 0.00 8.00 8.")).
	 * getByLabel("Expand Tax Jurisdiction").click();
	 * page.locator("#time-entry-20006").
	 * getByLabel("Attendance Hours entry for Tuesday").click();
	 * page.locator("#time-entry-20006").
	 * getByLabel("Attendance Hours entry for Tuesday").fill("0");
	 *
	 *
	 *
	 */
}
