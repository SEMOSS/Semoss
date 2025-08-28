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
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.BrowserType.LaunchOptions;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.BrowserChannel;
import com.microsoft.playwright.options.LoadState;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Playwrighter implements Runnable {

	int count = 0;
	public static final String css_id = "pw_id";
	public static final String starter = "You are a natural language to HTML element mapper giving output in the following format as a valid JSON array \n\n [{'pw_id of the element': 'element_value from the prompt provided '}, {'pw_id of the element': 'element_value from the prompt provided '} ] \n\n";
	// by their pw_id attribute that need to be filled. Provide element pw_id and
	// the potential value
	// in the format {'pw_id': 'field pw_id value'}. For html elements provide pw_id
	// attribute of the
	// html element and the potential value from the prompt. Identify all relevant
	// fields that can be
	// pre-filled. Dont identify un-necessary fields. If more than one element
	// provide as an array.
	public static final String json_format = "Identify the HTML elements that can satisfy the prompt << ";
	// USE THE CONTEXT PROVIDED ONLY. Do NOT manufacture facts. If this page does
	// not allow me to
	// accomplish my goal please respond with {result:NA}.
	public static final String no_disclaimer = "Be Concise. Do not provide as json markdown. Please provide only VALID. Do not provide introductions, explanations and summary. ";
	List inputList = new ArrayList();
	Map inputLinks = new Hashtable();
	String prompt = null;
	JavaRestClient jrc = null;
	Page cur_page = null;
	int counter = 0;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/*
		 * try (Playwright playwright = Playwright.create()) { Browser browser =
		 * playwright.webkit().launch(); Page page = browser.newPage();
		 * page.navigate("https://playwright.dev/"); page.screenshot(new
		 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example.png"))); }
		 */
		/*
		 * String json =
		 * "{'out':[{'pw_id=input1': 'newyork'}, {'pw_id=input9': '2024-12-01'}, {'pw_id=input11': '1 day'}, {'pw_id=input14': '1'}, {'pw_id=input15': 'false'}, {'pw_id=input16': 'false'}, {'pw_id=input12': '1'}, {'pw_id=input13': '0'}]}"
		 * ; JSONObject obj = new JSONObject(json);
		 * 
		 * //if(obj instanceof JSONArray) { System.err.println("Json array");
		 * 
		 * }
		 * 
		 * String jsonString =
		 * "{\"insightID\":\"684f8817-e41d-4cb2-a9b2-43ff7c1af4e2\",\"pixelReturn\":[{\"pixelId\":\"meta_unstored\",\"pixelExpression\":\"LLM(engine=[\\\"4801422a-5c62-421e-a00c-05c6a9e15de8\\\"], command=[\\\"I would like to travel to newyork for a meeting on december 1. Please provide output as json in the following json format with {pw_id attribute of the element: potential field value}. Try to identify pre-fill for as many fields as possible. Be Concise. Do not provide as markdown. Please provide only the JSON and as valid JSON. Do not provide introductions and summary. If this page does not allow me to accomplish my goal please respond with {result:NA}. --- HTML ELEMENTS FOLLOW -- \\n<button type=button class=uitk-layout-flex-item uitk-step-input-touch-target onclick=alert(document.querySelector([pw_id=button9]).value) pw_id=button9><span class=uitk-step-input-button>\\n  <svg class=uitk-icon uitk-step-input-icon aria-label=Decrease the number of children in room 1 role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n   <title id=traveler_selector_children_step_input-0-decrease-title>\\n    Decrease the number of children in room 1\\n   </title><path d=M19 13H5v-2h14v2z></path>\\n  </svg></span></button>\\n<button type=button class=uitk-layout-flex-item uitk-step-input-touch-target onclick=alert(document.querySelector([pw_id=button8]).value) pw_id=button8><span class=uitk-step-input-button>\\n  <svg class=uitk-icon uitk-step-input-icon aria-label=Increase the number of adults in room 1 role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n   <title id=traveler_selector_adult_step_input-0-increase-title>\\n    Increase the number of adults in room 1\\n   </title><path d=M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z></path>\\n  </svg></span></button>\\n<button type=button class=uitk-layout-flex-item uitk-step-input-touch-target onclick=alert(document.querySelector([pw_id=button7]).value) pw_id=button7><span class=uitk-step-input-button>\\n  <svg class=uitk-icon uitk-step-input-icon aria-label=Decrease the number of adults in room 1 role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n   <title id=traveler_selector_adult_step_input-0-decrease-title>\\n    Decrease the number of adults in room 1\\n   </title><path d=M19 13H5v-2h14v2z></path>\\n  </svg></span></button>\\n<button aria-roledescription=expand to change the number of travelers data-stid=open-room-picker aria-expanded=false aria-label=Travelers, 2 travelers, 1 room class=uitk-menu-trigger open-room-picker-observer-root uitk-fake-input uitk-form-field-trigger uitk-field-fake-input uitk-field-fake-input-hasicon type=button onclick=alert(document.querySelector([pw_id=button6]).value) pw_id=button6>2 travelers, 1 room</button>\\n<button data-testid=uitk-date-selector-input1-default data-stid=uitk-date-selector-input1-default name=EGDSDateRange-date-selector-trigger aria-invalid=false aria-describedby=4whajr-error aria-expanded=false aria-label=Dates, Nov 1 - Nov 4 class=uitk-fake-input uitk-form-field-trigger uitk-field-fake-input uitk-field-fake-input-hasicon type=button onclick=alert(document.querySelector([pw_id=button5]).value) pw_id=button5>Nov 1 - Nov 4</button>\\n<button aria-label=Where to? data-stid=destination_form_field-menu-trigger aria-expanded=false class=uitk-fake-input uitk-form-field-trigger uitk-field-fake-input uitk-field-fake-input-hasicon type=button onclick=alert(document.querySelector([pw_id=button4]).value) pw_id=button4></button>\\n<button title= tabindex=0 data-testid=header-menu-button data-context=global_navigation type=button class=uitk-button uitk-button-medium uitk-button-tertiary uitk-button-tertiary-large-icon uitk-spacing global-navigation-nav-button onclick=alert(document.querySelector([pw_id=button3]).value) pw_id=button3>Sign in</button>\\n<button data-context=global_navigation type=button class=uitk-button uitk-button-large uitk-button-tertiary uitk-button-only-icon global-navigation-nav-button onclick=alert(document.querySelector([pw_id=button2]).value) pw_id=button2>\\n <div class=uitk-layout-position uitk-layout-position-display-inline-block uitk-layout-position-relative>\\n  <svg class=uitk-icon uitk-icon-large aria-label=Communication Center icon role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n   <title id=comment-icon-title>\\n    Communication Center icon\\n   </title><path fill-rule=evenodd d=M20 2a2 2 0 0 1 1.99 2L22 22l-4-4H4a2 2 0 0 1-2-2V4c0-1.1.9-2 2-2h16zM6 14h12v-2H6v2zm12-3H6V9h12v2zM6 8h12V6H6v2z clip-rule=evenodd></path>\\n  </svg>\\n </div></button>\\n<button data-stid=carousel-nav-next aria-label=view next property themes tabindex=0 type=button class=uitk-button uitk-button-medium uitk-button-only-icon uitk-carousel-button-paging uitk-carousel-button-next uitk-button-paging onclick=alert(document.querySelector([pw_id=button14]).value) pw_id=button14>\\n <svg class=uitk-icon uitk-icon-leading uitk-icon-directional aria-label=view next property themes role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n  <title id=next-button-title>\\n   view next property themes\\n  </title><path d=M10 6 8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6-6-6z></path>\\n </svg></button>\\n<button data-stid=button-type-picker-trigger data-context=global_navigation type=button class=uitk-button uitk-button-medium uitk-button-tertiary uitk-spacing global-navigation-nav-button onclick=alert(document.querySelector([pw_id=button1]).value) pw_id=button1>\\n <svg class=uitk-icon uitk-spacing uitk-spacing-padding-inlineend-two uitk-icon-small aria-hidden=true viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n  <path d=M11.99 2A10 10 0 1 0 12 21.99 10 10 0 0 0 11.99 2zm6.93 6h-2.95a15.65 15.65 0 0 0-1.38-3.56A8.03 8.03 0 0 1 18.92 8zM12 4.04c.83 1.2 1.48 2.53 1.91 3.96h-3.82A14.09 14.09 0 0 1 12 4.04zM4.26 14a8.17 8.17 0 0 1 0-4h3.38a16.45 16.45 0 0 0 0 4H4.26zm.82 2h2.95c.32 1.25.78 2.45 1.38 3.56A7.99 7.99 0 0 1 5.08 16zm2.95-8H5.08a7.99 7.99 0 0 1 4.33-3.56A15.65 15.65 0 0 0 8.03 8zM12 19.96A14.09 14.09 0 0 1 10.09 16h3.82A14.09 14.09 0 0 1 12 19.96zM14.34 14H9.66a14.49 14.49 0 0 1 0-4h4.68a14.5 14.5 0 0 1 0 4zm.25 5.56c.6-1.11 1.06-2.31 1.38-3.56h2.95a8.03 8.03 0 0 1-4.33 3.56zM16.36 14c.16-1.34.16-2.66 0-4h3.38a8.17 8.17 0 0 1 0 4h-3.38z></path>\\n </svg>English</button>\\n<button id=search_button type=submit class=uitk-button uitk-button-large uitk-button-has-text uitk-button-primary onclick=alert(document.querySelector([pw_id=button13]).value) pw_id=button13>Search</button>\\n<button title=Shop travel tabindex=0 data-testid=header-menu-button data-context=global_navigation type=button class=uitk-button uitk-button-medium uitk-button-tertiary uitk-spacing global-navigation-nav-button onclick=alert(document.querySelector([pw_id=button0]).value) pw_id=button0>\\n <div aria-hidden=true>\\n  Shop travel\\n </div>\\n <svg class=uitk-icon uitk-icon-small aria-describedby=header-menu-expand_more-description role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n  <desc id=header-menu-expand_more-description>\\n   Shop travel\\n  </desc><path d=M16.59 8.59 12 13.17 7.41 8.59 6 10l6 6 6-6-1.41-1.41z></path>\\n </svg></button>\\n<button id=traveler_selector_done_button type=button class=uitk-button uitk-button-medium uitk-button-has-text uitk-button-primary onclick=alert(document.querySelector([pw_id=button12]).value) pw_id=button12>Done</button>\\n<button id=traveler_selector_add_room data-test-id=traveler_selector_add_room data-context=uitk-form-context type=button class=uitk-button uitk-button-medium uitk-button-has-text uitk-button-tertiary onclick=alert(document.querySelector([pw_id=button11]).value) pw_id=button11>Add another room</button>\\n<button type=button class=uitk-layout-flex-item uitk-step-input-touch-target onclick=alert(document.querySelector([pw_id=button10]).value) pw_id=button10><span class=uitk-step-input-button>\\n  <svg class=uitk-icon uitk-step-input-icon aria-label=Increase the number of children in room 1 role=img viewBox=0 0 24 24 xmlns=http://www.w3.org/2000/svg xmlns:xlink=http://www.w3.org/1999/xlink>\\n   <title id=traveler_selector_children_step_input-0-increase-title>\\n    Increase the number of children in room 1\\n   </title><path d=M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z></path>\\n  </svg></span></button>\\n<input type=hidden data-stid=EGDSDateRangePicker-StartDate name=EGDSDateRangePicker-StartDate-date_form_field value=2024-11-01 onchange=alert(document.querySelector([pw_id=input9]).value) pw_id=input9>\\n<input type=hidden data-stid=EGDSDatePickerFlexibilityCalendarContent-SelectedOption name=EGDSDatePickerFlexibilityCalendarContent-SelectedOption-date_form_nested_flexible_field value=0_DAY onchange=alert(document.querySelector([pw_id=input8]).value) pw_id=input8>\\n<input type=hidden data-stid=EGDSSoftPackagesPackageType name=EGDSSoftPackagesPackageType value= onchange=alert(document.querySelector([pw_id=input17]).value) pw_id=input17>\\n<input type=text id=346r3 value=Nov 1 - Nov 4 class=uitk-field-input is-hidden placeholder=Placeholder aria-required=false aria-invalid=false onchange=alert(document.querySelector([pw_id=input7]).value) pw_id=input7>\\n<input type=checkbox name=add-car-switch id=soft_packages_car_pill class=uitk-layout-flex-item uitk-layout-flex-item-flex-shrink-0 aria-required=false aria-label=Add a car value= onchange=alert(document.querySelector([pw_id=input16]).value) pw_id=input16>\\n<input type=hidden data-stid=EGDSSearchFormLocationField-Selected name=EGDSSearchFormLocationField-Selected-destination_form_field value= onchange=alert(document.querySelector([pw_id=input6]).value) pw_id=input6>\\n<input type=checkbox name=add-flight-switch id=soft_packages_flight_pill class=uitk-layout-flex-item uitk-layout-flex-item-flex-shrink-0 aria-required=false aria-label=Add a flight value= onchange=alert(document.querySelector([pw_id=input15]).value) pw_id=input15>\\n<input type=hidden data-stid=EGDSSearchFormLocationField-Long name=EGDSSearchFormLocationField-Long-destination_form_field value= onchange=alert(document.querySelector([pw_id=input5]).value) pw_id=input5>\\n<input type=hidden data-stid=EGDSSearchFormTravelersField-Adult-1 name=EGDSSearchFormTravelersField-Adult-Room1 value=2 onchange=alert(document.querySelector([pw_id=input14]).value) pw_id=input14>\\n<input type=hidden data-stid=EGDSSearchFormLocationField-Lat name=EGDSSearchFormLocationField-Lat-destination_form_field value= onchange=alert(document.querySelector([pw_id=input4]).value) pw_id=input4>\\n<input type=text id=traveler_selector_children_step_input-0 min=0 max=6 tabindex=-1 aria-label=Children,Ages 0 to 17 class=uitk-layout-flex-item uitk-step-input-value value=0 readonly onchange=alert(document.querySelector([pw_id=input13]).value) pw_id=input13>\\n<input type=hidden data-stid=EGDSSearchFormLocationField-AirportCode name=EGDSSearchFormLocationField-AirportCode-destination_form_field value= onchange=alert(document.querySelector([pw_id=input3]).value) pw_id=input3>\\n<input type=text id=traveler_selector_adult_step_input-0 min=1 max=14 tabindex=-1 aria-label=Adults class=uitk-layout-flex-item uitk-step-input-value value=2 readonly onchange=alert(document.querySelector([pw_id=input12]).value) pw_id=input12>\\n<input type=hidden data-stid=EGDSSearchFormLocationField-Location name=EGDSSearchFormLocationField-Location-destination_form_field value= onchange=alert(document.querySelector([pw_id=input2]).value) pw_id=input2>\\n<input type=text class=uitk-field-input is-hidden placeholder=Placeholder aria-required=false aria-invalid=false value=2 travelers, 1 room onchange=alert(document.querySelector([pw_id=input11]).value) pw_id=input11>\\n<input type=text aria-label=Where to? value= id=destination_form_field-input name=destination_form_field class=uitk-field-input empty-placeholder aria-required=false aria-invalid=false onchange=alert(document.querySelector([pw_id=input1]).value) pw_id=input1>\\n<input type=hidden data-stid=EGDSDateRangePicker-EndDate name=EGDSDateRangePicker-EndDate-date_form_field value=2024-11-04 onchange=alert(document.querySelector([pw_id=input10]).value) pw_id=input10>\\n<input type=text class=uitk-field-input is-hidden empty-placeholder placeholder= aria-required=false aria-invalid=false value= onchange=alert(document.querySelector([pw_id=input0]).value) pw_id=input0>\\\"]););\",\"isMeta\":true,\"timeToRun\":-1,\"output\":\"[77,206] expecting: EOF. Error in syntax around attribute of the el\",\"operationType\":[\"ERROR\",\"INVALID_SYNTAX\"]}]}"
		 * ;
		 */
		for (int tIndex = 0; tIndex < 1; tIndex++) {
			Playwrighter t = new Playwrighter();
			// t.getOutput(jsonString);

			// t.callLLM("hello", null);

			t.count = tIndex;
			Thread t1 = new Thread(t);
			System.err.println("Starting thread.. " + tIndex);
			t1.start();
		}
	}

	public void initJRC() {
		jrc = new JavaRestClient();
		jrc.login("https://workshop.cfg.deloitte.com/cfg-ai-demo/Monolith/api", "488c161f-ba17-45ba-97a1-22915fb17f15",
				"43f214d4-c0fa-4587-9d41-438463d77a6d");
		// jrc.login("https://workshop.cfg.deloitte.com/cfg-ai-demo/Monolith/api",
		// "abcd", "xyz");
		// String py = "Py('<encode> 2 + 2</encode>')";
		// String llm = "LLM(engine='4801422a-5c62-421e-a00c-05c6a9e15de8',
		// command='hello')";
	}

	public void run() {
		// runJSoup();
		// runPage();
		// String content = readData("c:/temp/expedia.html");

		String baseUrl = "https://play.semoss.org/dev/SemossWeb/";
		prompt = "I would like to travel to newyork for a meeting on december 1 2024 for a day. I am traveling alone. ";
		prompt = "I would like to login with username: prabhu and password: neelsanghvi";
		// baseUrl = "https://www.expedia.com/";
		baseUrl = "https://www.facebook.com/login";
		// baseUrl = "https://www.kayak.com/";
		// runExpedia();
		// String newContent = convertRelativeToAbsoluteLinks(baseUrl, content);
		// writeData(newContent);
		// baseUrl = "https://www.hotwire.com/";
		// baseUrl = "https://www.makemytrip.com/";

		loadPage(baseUrl);
		String finalPrompt = makePrompt();
		initJRC();
		String response = callLLM(finalPrompt, null);
		JSONArray output = getOutput(response);
		System.err.println("Potential Elements ..." + output);

		Map outMap = convertJSONArrayToMap(output);
		// get feedback on the elements
		Map setterMap = getUserChoices(outMap);

		executePage(setterMap);
	}

	private String makePrompt() {
		// take the basic prompt
		// take the input elements and then convert it into a prompt
		Iterator<String> keys = inputLinks.keySet().iterator();
		StringBuffer inputMaker = new StringBuffer();
		for (int elemIndex = 0; elemIndex < inputList.size(); elemIndex++) {
			String key = (String) inputList.get(elemIndex);
			inputMaker.append("   ");
			inputMaker.append(inputLinks.get(key));
		}
		String finalPrompt = starter + json_format + prompt + ">>" + "--- HTML ELEMENTS START -- " + inputMaker
				+ "--- HTML ELEMENTS END -- " + no_disclaimer;
		System.err.println("---");
		System.err.println(finalPrompt);
		System.err.println("---");
		return finalPrompt;
	}

	public void loadPage(String baseUrl) {
		try {
			Playwright pw = Playwright.create();
			Browser context = pw.webkit().launch();
			Page page = context.newPage();
			page.navigate(baseUrl);
			page.waitForLoadState(LoadState.DOMCONTENTLOADED);
			Thread.sleep(3000);
			// page.onDialog(dialog -> dialog.dismiss());
			// page.getByText("Accept").click();
			String content = page.content();
			String title = baseUrl.replace("https://", "").replace("http://", "").replace(".", "_").replace("/", "__");
			content = convertRelativeToAbsoluteLinks(baseUrl, content);
			System.out.println("--------------------");
			FileUtils.write(new File("c:/temp/" + title + ".html"), content);
			System.err.println("Finished.. ");
			cur_page = page;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public String callLLM(String finalPrompt, String engineId) {
		finalPrompt = finalPrompt.replace("\"", "");
		// finalPrompt = finalPrompt.replace("\'", "");
		engineId = "4801422a-5c62-421e-a00c-05c6a9e15de8";
		// engineId = "029a1323-db79-415c-be3e-3945438b0808";
		String llm = "LLM(engine=[\"" + engineId + "\"], command=[\"<encode>" + finalPrompt + "</encode>\"]);";
		System.err.println(llm);
		String response = jrc.runPixel(llm);
		return response;
		/*
		 * for(int outputIndex = 0;outputIndex < output.length();outputIndex++) {
		 * JSONObject curObject = output.getJSONObject(outputIndex);
		 * if(curObject.has(css_id)) { String pw_id = curObject.getString(css_id);
		 * String pot_value = curObject.getString("potential_value");
		 * 
		 * 
		 * } }
		 */

		// System.err.println(response);

		// extract output
		// get into json
		// see if this is result na
		// if not walk through input elements and replay
	}

	private Map getUserChoices(Map llmMap) {
		// this is where we get choices from the user
		StringBuffer itemList = new StringBuffer();
		Iterator keys = inputLinks.keySet().iterator();
		while (keys.hasNext())
			itemList.append(keys.next()).append("<>");
		Map newMap = new HashMap();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String elemSelected = null;
		String value = null;
		try {
			do {
				System.err.println("Select an Element : " + itemList);
				elemSelected = br.readLine();
				String elemDetails = inputLinks.get(elemSelected).toString();
				System.err.println("Element Details..  " + elemDetails);
				if (llmMap.containsKey(elemSelected)) {
					value = (String) llmMap.get(elemSelected);
					System.err.println("Proposed Value.. " + value);
				} else
					System.err.println("No value proposed.. please enter value");
				value = br.readLine(); // type i for ignore
				if (!value.equalsIgnoreCase("i"))
					newMap.put(elemSelected, value);

			} while (!elemSelected.equalsIgnoreCase("i"));
			// while()
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return newMap;
	}

	public void executePage(Map valueMap) {
		// need to ask the user for button to hit or the link to click
		// set the values and roll

		// get the current page
		// set the values
		// click the button
		Iterator<String> valueKeys = valueMap.keySet().iterator();
		System.err.println("Going to fill " + valueMap.size() + " elements");
		while (valueKeys.hasNext()) {
			String valueKey = valueKeys.next();
			String value = valueMap.get(valueKey).toString();
			Element elem = (Element) inputLinks.get(valueKey);
			// get the id of this elem ?
			// page.locator("input[name=\"userLoginName\"]").fill("prabhuk");
			String elemName = elem.attr("name");
			if (elemName.length() > 0) {
				String selector = "input[name='" + elemName + "']";
				Locator locator = cur_page.locator(selector);
				if (locator != null) {
					System.err.println("Found locator.. ");
					locator.fill(value);
				}
			}
		}

		// finally click the button

	}

	private JSONArray getOutput(String jsonString) {
		JSONObject obj = new JSONObject(jsonString);
		JSONArray retObject = (JSONArray) obj.get("pixelReturn");
		JSONObject outputObj = retObject.getJSONObject(0);
		JSONObject output = outputObj.getJSONObject("output");

		System.err.println(" output.. " + output);
		System.err.println(output.get("response"));

		String new_obj = output.get("response") + "";
		new_obj = "{'out': " + new_obj + "}";

		JSONObject ret_obj = new JSONObject(new_obj);

		return ret_obj.getJSONArray("out");
	}

	private void replacer(String baseUrl, Elements links, String attr) {
		try {
			for (Element link : links) {
				String href = link.attr(attr);
				System.err.println("Old Link " + href);
				if (href.startsWith("/") || href.startsWith("./") || href.startsWith("../")
						|| !href.startsWith("http")) {
					URI baseUri = new URI(baseUrl);
					URI linkUri = new URI(href);
					URI absoluteUri = baseUri.resolve(linkUri);
					link.attr(attr, absoluteUri.toString());
					System.err.println("New Link " + absoluteUri.toString());
				}
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String convertRelativeToAbsoluteLinks(String baseUrl, String page) {
		// Base URL of the document (you might need to adjust this based on your actual
		// use case)

		// Parse the HTML document
		Document doc;
		try {
			doc = Jsoup.parse(page);

			// Find all links
			Elements links = doc.select("[href]");
			replacer(baseUrl, links, "href");

			// replace all image sources / javascripts
			links = doc.select("[src]");
			replacer(baseUrl, links, "src");

			// add events and ids to objects
			// add to the buttons
			links = doc.select("button");
			makeObservable(links, "button", "onclick");

			links = doc.select("a");
			// makeObservable(links, "hyper", "onclick");

			// put all the inputs
			links = doc.select("input");
			makeObservable(links, "input", "onchange");

			// also capture all the links / elements
			// printElements(links);
			links = doc.select("button");

			// Convert relative links to absolute
			return doc.html();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Print the updated HTML document
		// System.out.println(doc.html());
		return null;
	}

	private void makeObservable(Elements links, String name, String event) {

		for (int linkIndex = 0; linkIndex < links.size(); linkIndex++) {
			Element link = links.get(linkIndex);
			link.attr(css_id, name + linkIndex);
			link.attr(event, "alert(document.querySelector('[" + css_id + "=" + name + linkIndex + "]').value)");
			link.removeAttr("disabled");
			String id = link.attr(css_id);
			inputLinks.put(id, link);
			inputList.add(id);

			// get the id and name also
			if (link.hasAttr("id"))
				inputLinks.put(link.attr("id"), link);
			if (link.hasAttr("name"))
				inputLinks.put(link.attr("name"), link);
		}
	}

	private Map convertJSONArrayToMap(JSONArray arr) {

		// {"element_pw_id": "input14", "element_value": "2"},
		Map retMap = new HashMap();
		String keyName = "pw_id";
		String valueName = "value";
		for (int arrIndex = 0; arrIndex < arr.length(); arrIndex++) {
			JSONObject obj = arr.getJSONObject(arrIndex);
			Iterator<String> objKeys = obj.keys();
			String elemId = null;
			String elemValue = null;

			while (objKeys.hasNext()) {
				String key = objKeys.next();
				String value = obj.get(key) + "";

				if (key.contains("="))
					key = key.split("=")[1];

				if (value.contains("="))
					value = value.split("=")[1];

				if (key.contains(css_id)) {
					if (elemId == null) {
						if (inputLinks.containsKey(key)) {
							elemId = key;
							if (elemValue == null)
								elemValue = value;
						} else if (inputLinks.containsKey(value))
							elemId = value;
					}
				} else
					elemValue = value;
			}
			retMap.put(elemId, elemValue);
		}
		// if(obj.has(key))
		// {
		// String elemId = obj.getString(key);
		// if(elemId != null && inputLinks.containsKey(elemId) && obj.has(value))
		// {
		// String elemValue = obj.getString(value);
		// retMap.put(elemId, elemValue);
		// }
		// }
		// }
		return retMap;
	}

	public void runJSoup(String content) {
		try {
			Document doc = null;
			if (content == null) {
				String urlLoc = "http://www.semoss.org";
				urlLoc = "https://jsoup.org/cookbook/extracting-data/example-list-links";
				urlLoc = "https://play.semoss.org/dev/SemossWeb/#!/login";
				URL url = new URL(urlLoc);

				doc = Jsoup.connect(urlLoc).get();
			} else {
				doc = Jsoup.parse(content);
			}
			Elements all_els = doc.getAllElements();
			for (int elIndex = 0; elIndex < all_els.size(); elIndex++) {
				Element ele = all_els.get(elIndex);
				List<Attribute> attrs = ele.attributes().asList();
				for (int attrIndex = 0; attrIndex < attrs.size(); attrIndex++) {
					System.err.println(attrs.get(attrIndex));
				}
				System.err.println(ele.text() + "<>" + ele.html());
			}

			Elements els = doc.select("a[href]");
			for (int elIndex = 0; elIndex < els.size(); elIndex++) {
				Element ele = els.get(elIndex);
				List<Attribute> attrs = ele.attributes().asList();
				for (int attrIndex = 0; attrIndex < attrs.size(); attrIndex++) {
					System.err.println(attrs.get(attrIndex));
				}
				System.err.println(ele.attr("href") + "<>" + ele.text() + "<>" + ele.html());
			}

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String readData(String fileName) {
		try {
			return FileUtils.readFileToString(new File(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private void writeData(String data) {
		try {
			FileUtils.write(new File("c:/temp/loginpage2.html"), data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void runPW() {
		try (Playwright playwright = Playwright.create()) {
			Browser browser = playwright.webkit().launch();
			Page page = browser.newPage();
			page.navigate("http://www.gmail.com/");
			List<Locator> allLocs = page.getByRole(AriaRole.TEXTBOX).all();

			for (int locIndex = 0; locIndex < allLocs.size(); locIndex++) {
				Locator thisLoc = allLocs.get(locIndex);
				System.err.println(thisLoc.textContent());
				System.out.println(allLocs.get(locIndex));
			}
			Locator loc = page.getByText("DOWNLOAD").first();
			// Locator loc = page.getByRole(AriaRole.LINK, new
			// Page.GetByRoleOptions().setName("Download")).first();
			loc.click();
			System.err.println(page.content());
			page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count + ".png")));
		}
	}

	public void runExpedia() {

		try (Playwright playwright = Playwright.create()) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

			Playwright pw = playwright;

			LaunchOptions lp = new LaunchOptions();
			lp.setChannel(BrowserChannel.CHROME);
			lp.setHeadless(false);
			BrowserType firefox = pw.chromium();
			Browser context = firefox.launch();
			// Page page = browser.newPage();

			// Browser context = pw.webkit().launch();
			Page page = context.newPage();
			String baseUrl = "https://www.hotwire.com/";
			// baseUrl = "https://ecommerce-playground.lambdatest.io/";
			page.navigate(baseUrl);
			Thread.sleep(2000);
			String data = null;
			System.err.println("Enter locator");
			do {
				Locator where = null;
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
				String locator = "//*[@data-stid=\"destination_form_field-menu-trigger\"]";
				String selector = "Where would you like to stay?";
				String selector2 = "//*[@id=\"lodging_search_form\"]/div/div/div[1]/div/div/div[1]/button";
				// selector = "pierce/#lodging_search_form > div > div > div:nth-of-type(1)
				// button";
				where = page.getByPlaceholder(selector);
				where.fill("WAS");
				// where.click();
				// where.press("Enter");

				where.press("ArrowDown");
				where.press("Enter");
				where.press("Escape");
				// page.locator(".farefinder-container").click();
				Thread.sleep(1000);
				// page.keyboard().

				// ElementHandle handle = page.querySelector(selector);
				// handle.click();
				// Thread.sleep(1000);
				// handle = page.querySelector(selector2);
				// System.err.println("Found it.. " + where.getAttribute("class"));
				// where = page.locator(locator);
				Locator search = page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Find a hotel"));
				System.err.println("Visibility.. " + search.isVisible() + search.isEnabled());
				// where.vi
				search.click();
				// while (where.isVisible())
				// where.click();
				Thread.sleep(3000);
				page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count + ".png")));
				FileUtils.write(new File("c:/temp/popup.html"), page.content());

			} while ((data = br.readLine()) != null);

			// page.onDialog(dialog -> dialog.dismiss());
			// page.getByText("Accept").click();
			Locator where = page.locator("button[data-stid=\"destination_form_field-menu-trigger\"]");
			where.click();
			System.err.println("clicked where to");
			Thread.sleep(2000);
			// this is where I need to get to the locator handler
			where.locator("input[data-stid=\"destination_form_field-dialog-input\"]").fill("WAS");
			page.locator("input[data-stid=\"destination_form_field-dialog-input\"]").press("Enter");
			Thread.sleep(3000);
			page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count + ".png")));

			// page.locator("input[name=\"userLoginName\"]").fill("prabhuk");
			// page.locator("input[name=\"userLoginPassword\"]").fill("S3m0ss!23");

			// runJSoup(content);
			// System.err.println(page.content());

			/*
			 * List<ElementHandle> locList = page.querySelectorAll(" "); for(int locIndex =
			 * 0;locIndex< locList.size();locIndex++) { ElementHandle thisElement =
			 * locList.get(locIndex); System.err.println(thisElement.textContent()); }
			 */
			// cookie.getByLabel("Accept").click();
			// System.err.println(page.getByText(("Notice:")));

			// gets all the popups..
			/*
			 * page.onPopup(popup -> { popup.waitForLoadState();
			 * System.out.println(popup.title()); });
			 */

			// Page popup = page.waitForPopup(() -> {page.getByText("open the
			// popup").click();});
			// popup.getByRole(AriaRole.BUTTON).click();

			// page.waitForLoadState();

			/*
			 * // Interact with login form
			 * page.locator("input[name=\"userLoginName\"]").fill("prabhuk");
			 * page.locator("input[name=\"userLoginPassword\"]").fill("S3m0ss!23");
			 * page.getByRole(AriaRole.BUTTON, new
			 * Page.GetByRoleOptions().setName("Log In").setExact(true)).click(); //
			 * Pattern.compile("Log In", Pattern.CASE_INSENSITIVE)) Thread.sleep(3000);
			 * //page.wait(3000);
			 * //page.navigate("https://play.semoss.org/dev/SemossWeb/#!/");
			 * System.err.println("Finished fill"); //List <Locator> locators =
			 * page.getByRole(AriaRole.FORM).all(); //System.err.println(locators.size());
			 * //page.waitForSelector("input[type="username"]", { visible: true });
			 * //page.getByLabel("Password").fill("S3m0ss!23");
			 * //page.getByRole(AriaRole.BUTTON, new
			 * Page.GetByRoleOptions().setName("Log in")).click(); //FileWriter fw = new
			 * FileWriter(new StringWriter()); content = page.content(); content =
			 * convertRelativeToAbsoluteLinks(baseUrl, content); FileUtils.write(new
			 * File("c:/temp/semosspage.html"), content); page.screenshot(new
			 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
			 * ".png")));
			 */
			System.err.println("Finished.. ");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void runPage() {
		try {
			Playwright pw = Playwright.create();
			Browser context = pw.webkit().launch();
			Page page = context.newPage();
			String baseUrl = "https://play.semoss.org/dev/SemossWeb/";
			page.navigate("https://play.semoss.org/dev/SemossWeb/#!/login");
			page.onDialog(dialog -> dialog.dismiss());
			page.getByText("Accept").click();
			String content = page.content();
			content = convertRelativeToAbsoluteLinks(baseUrl, content);
			System.out.println("--------------------");
			FileUtils.write(new File("c:/temp/loginpage.html"), content);

			// runJSoup(content);
			// System.err.println(page.content());

			/*
			 * List<ElementHandle> locList = page.querySelectorAll(" "); for(int locIndex =
			 * 0;locIndex< locList.size();locIndex++) { ElementHandle thisElement =
			 * locList.get(locIndex); System.err.println(thisElement.textContent()); }
			 */
			// cookie.getByLabel("Accept").click();
			// System.err.println(page.getByText(("Notice:")));

			// gets all the popups..
			/*
			 * page.onPopup(popup -> { popup.waitForLoadState();
			 * System.out.println(popup.title()); });
			 */

			// Page popup = page.waitForPopup(() -> {page.getByText("open the
			// popup").click();});
			// popup.getByRole(AriaRole.BUTTON).click();

			// page.waitForLoadState();

			/*
			 * // Interact with login form
			 * page.locator("input[name=\"userLoginName\"]").fill("prabhuk");
			 * page.locator("input[name=\"userLoginPassword\"]").fill("S3m0ss!23");
			 * page.getByRole(AriaRole.BUTTON, new
			 * Page.GetByRoleOptions().setName("Log In").setExact(true)).click(); //
			 * Pattern.compile("Log In", Pattern.CASE_INSENSITIVE)) Thread.sleep(3000);
			 * //page.wait(3000);
			 * //page.navigate("https://play.semoss.org/dev/SemossWeb/#!/");
			 * System.err.println("Finished fill"); //List <Locator> locators =
			 * page.getByRole(AriaRole.FORM).all(); //System.err.println(locators.size());
			 * //page.waitForSelector("input[type="username"]", { visible: true });
			 * //page.getByLabel("Password").fill("S3m0ss!23");
			 * //page.getByRole(AriaRole.BUTTON, new
			 * Page.GetByRoleOptions().setName("Log in")).click(); //FileWriter fw = new
			 * FileWriter(new StringWriter()); content = page.content(); content =
			 * convertRelativeToAbsoluteLinks(baseUrl, content); FileUtils.write(new
			 * File("c:/temp/semosspage.html"), content); page.screenshot(new
			 * Page.ScreenshotOptions().setPath(Paths.get("c:/temp/example" + count +
			 * ".png")));
			 */
			System.err.println("Finished.. ");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
