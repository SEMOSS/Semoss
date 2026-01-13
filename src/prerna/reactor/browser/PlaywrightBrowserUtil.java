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
package prerna.reactor.browser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.FormElement;
import org.jsoup.select.Elements;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Download;
import com.microsoft.playwright.Keyboard;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Locator.FilterOptions;
import com.microsoft.playwright.Mouse;
import com.microsoft.playwright.Mouse.ClickOptions;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Page.GetByRoleOptions;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.PlaywrightException;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.MouseButton;
import com.microsoft.playwright.options.ViewportSize;

import prerna.util.Utility;


public class PlaywrightBrowserUtil {
	
	Playwright playwright = null;
	public static final String css_id = "pw_id";
	Map formInputs = new HashMap<String, List>();
	Map formButton = new HashMap<String, Element>();
	List inputList = new ArrayList();
	Map inputLinks = new Hashtable();
	Map <String, Locator> locators = new HashMap();
	Map <String, String> variables = new HashMap();
	String outputDir = null; 
	String session = null;
	String baseUrl = null;
	int sleep = 400;
	BrowserContext ctx = null;
	JSONObject root = new JSONObject();
	boolean capture = true;
	int cur_width = 0;
	int cur_height = 0;
	int user_width = 1516;
	int user_height = 692;
	
	/*
	{
	    'actor':'system',
	    'action':'navigate',
	    'website':'https://dte.deloittenet.com',
	    'website3':'https://deloittenet.deloitte.com',
	    'website2': 'https://login.microsoftonline.com/36da45f1-dd2c-4d1f-af13-5abe46b99921/wsfed/?wa=wsignin1.0&wtrealm=urn%3adeloittenet%3asharepoint&wctx=https%3a%2f%2fdeloittenet.deloitte.com%2f_layouts%2f15%2fAuthenticate.aspx%3fSource%3d%252F&sso_reload=true',
	  }
		 */
	
	public static final String ACTION = "action";
	public static final String ACTOR = "actor";
	public static final String WEBSITE = "website";
	public static final String EVENT = "event";
	public static final String PARAMS = "params";
	public static final String OPTIONS = "options";
	public static final String OPTION_VALUES = "option_values";
	public static final String OPTION_EXACT = "option_exact";
	public static final String SHIFT = "SHIFT";
	public static final String CTRL = "CTRL";
	
	
	boolean tracePage = false;
	boolean traceScreenshot = false;
	
	private Page cur_page = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// read the file
		// for every action do the appropriate action
		String fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/playwright.json";
		fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/timesheet.json";
		fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/downloader.json";
		fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/timesheet_time_approval.json";
		
		PlaywrightBrowserUtil pw = new PlaywrightBrowserUtil();
		pw.tracePage = true;
		pw.traceScreenshot = true;
		pw.initPlaywright();
		pw.mimicUser();
		pw.processFile(fileName);
	}
	
	/** functions we need
	 * open
	 * close
	 * click xy
	 * get all forms
	 * get all input fields
	 * get all the links
	 * get screenshot
	 * click link
	 * upload - wow this will be interesting.. 
	 * 
	 */
	public void open(JSONObject obj)
	{
		if(ctx == null)
			initPlaywright();
		
		navigate(obj);
	}
	
	public void close() {
		cur_page.close();
		ctx.close();
		cur_page = null;
		ctx = null;
	}
	
	public void initPlaywright()
	{
	     //LaunchOptions lp = new LaunchOptions();
	     //lp.setChannel(BrowserChannel.CHROME);
	     //lp.setHeadless(false);
		String tempString = System.getProperty("java.io.tmpdir");
		Path p = Paths.get(tempString);
		this.outputDir = p.toAbsolutePath().toString();
		
		if(this.playwright == null)
			this.playwright = Playwright.create();
	     
	     BrowserType firefox = playwright.chromium();
	     //BrowserType firefox = playwright.webkit();
	     Browser browser = firefox.launch();
	     ctx = browser.newContext();
	     //page = ctx.newPage();
	     cur_page = ctx.newPage();
	     
	     session = java.util.UUID.randomUUID() +"";
	     
	     // default sleep value to 200
	     this.sleep = 200;
	     
	    //ViewportSize vs = cur_page.viewportSize();
	    cur_page.setViewportSize(user_width, user_height);
	    ViewportSize vs = cur_page.viewportSize();
	    System.err.println(vs.width + " <<>>" + vs.height);
	    cur_width = vs.width;
	    cur_height = vs.height;
		    
	}
	
	public void setUserWidthHeight(int width, int height)
	{
		user_width = width;
		user_height = height;
	}

	
	public void mimicUser()
	{
		try {
			boolean done = false;
			while(!done)
			{
				JSONObject obj = mimicAction();
				if(obj.getString("actor").equalsIgnoreCase("qq"))
					break;
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				System.err.println("Enter a task name");
				String actionId = br.readLine();
				processAction(obj, actionId);
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public JSONObject mimicAction()
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		// get the event
		// get the details
		String data = null;
		String [] keys = new String[] {"actor", "action", "website", "event", "params"};
		Map kv = new HashMap();
		try {
			for(int keyIndex = 0;keyIndex < keys.length;keyIndex++)
			{
				String key = keys[keyIndex];
				Object value = null;
				System.err.println(key);
				if(key.equalsIgnoreCase("website"))
				{
					if(baseUrl == null)
						value = br.readLine();
					else 
						continue;
					kv.put(key, value);
				}
				else if(key.equalsIgnoreCase("params"))
				{
					List paramList = new ArrayList();
					String type = "string";
					while(!(data=br.readLine()).equalsIgnoreCase("q"))
					{
						try {
							int val = Integer.parseInt(data);
							type = "int";
							paramList.add(val);
						} catch (NumberFormatException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							paramList.add(data);
						}
					}
					if(type.equalsIgnoreCase("string"))
					{
						String []strVal = new String[paramList.size()];
						for(int valIndex = 0;valIndex < paramList.size();valIndex++)
							strVal[valIndex] = (String)paramList.get(valIndex);
						kv.put(key, strVal);
					}
					else
					{
						int [] intVal = new int[paramList.size()];
						for(int valIndex = 0;valIndex < paramList.size();valIndex++)
							intVal[valIndex] = (Integer)paramList.get(valIndex);
						kv.put(key, intVal);
					}
				}
				else 
				{
					value = br.readLine();
					kv.put(keys[keyIndex], value);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// forcing shift
		kv.put(SHIFT, true);
		
		JSONObject obj = new JSONObject(kv);
		System.out.println(obj);
		
		return obj;
	}
	

	
	
	
	public void processFile(String fileName)
	{
		try {
			String jsonData = FileUtils.readFileToString(new File(fileName), Charset.defaultCharset());
			JSONObject root = new JSONObject(jsonData);
			JSONArray actions = root.getJSONArray("actions");
			
			System.err.println(actions);
			boolean go = true;
			for(int actionIndex = 0;actionIndex < actions.length() && go;actionIndex++)
			{
				String actionId = actions.getString(actionIndex);
				JSONObject action = root.getJSONObject(actionId);
				String actionName = action.getString("action");
				String actor = action.getString("actor");
				go = processAction(action, actionId);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private boolean processAction(JSONObject action, String actionId)
	{
		String actionName = action.getString("action");
		boolean go = true;
		String actor = action.getString("actor");
		boolean critical = action.has("critical") && action.getString("critical").equalsIgnoreCase("True");
		System.err.println("Processing ::: " + actionId);
		if(actor.equalsIgnoreCase("system"))
		{
			try {
				if(actionName.equalsIgnoreCase("navigate"))
				{
					navigate(action);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("getByPlaceHolder"))
				{
						getByPlaceHolder(action, actionId);
						handleSleep(action);
						trace(actionId, actionName);
					
				}
				if(actionName.equalsIgnoreCase("locator"))
				{
					locator(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("getByRole"))
				{
					getByRole(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("getByLabel"))
				{
					getByLabel(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("getByText"))
				{
					getByText(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("filter"))
				{
					doFilter(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("download"))
				{
					downloadFile(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("clickXY"))
				{
					mouse_xy(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("hoverXY"))
				{
					mouse_xy(action, actionId);
					handleSleep(action);
					trace(actionId, actionName);
				}
				if(actionName.equalsIgnoreCase("getInputs"))
				{
					System.err.println(getInputs());
				}
				if(actionName.equalsIgnoreCase("screenshot"))
				{
					getScreenShot();
					
				}
				if(actionName.equalsIgnoreCase("getUrl"))
				{
					getUrl();
					
				}
				if(actionName.equalsIgnoreCase("getHTML"))
				{
					getHTML();
					
				}
				if(actionName.equalsIgnoreCase("get"))
				{
					System.err.println(getChoices(actionId));
				}
				if(actionName.equalsIgnoreCase("keypress"))
				{
					keyboard(action, actionId);
				}
		// other cases follow} 
			}
			catch (Exception e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
				if(!critical)
					System.err.println("Failed " + actionId);
				else
				{
					go = false;
				}
			}
		}
		
		else if(actor.equalsIgnoreCase("user"))
		{
			getUserInput(action, actionId);
			handleSleep(action);
			trace(actionId, actionName);
		}
		else if(actor.equalsIgnoreCase("pause"))
		{
			go = pause(action, actionId);
		}
		else if(actor.equalsIgnoreCase("trace"))
		{
			// this is where trace should go.. 
		}
		
		if(action.has("debug") && action.getString("debug").equalsIgnoreCase("true") && locators.containsKey(actionId))
			System.err.println(locators.get(actionId).innerHTML());
		
		return go;
	}
	
	
	public void navigate(JSONObject payload)
	{
		if(ctx == null)
			initPlaywright();
		
		String website = payload.getString("website");
		this.baseUrl = website;
		cur_page.navigate(website);
		handleSleep(payload);
	}
	
	public void getByPlaceHolder(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		String placeholder = params.get(0).toString();
		String parent_action = null;
		Locator loc = null;
		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			loc = locators.get(parent_action).getByPlaceholder(placeholder);
		}
		else
		{
			loc = cur_page.getByPlaceholder(placeholder);
		}
		locators.put(actionName, loc);
		
	    if(payload.has("event"))
	    {
		    String event = payload.getString("event");
		    // another event loop goes here
		    if(event.equalsIgnoreCase("click"))
		    	loc.click();
		    if(event.equalsIgnoreCase("fill"))
		    {
		    	String fillValue = payload.getString("fill_value");
		    	loc.fill(fillValue);
		    }	
	    }
	}

	public void locator(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		String placeholder = params.get(0).toString();
		String parent_action = null;
		Locator loc = null;
		List <Locator> allLoc = null;
		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			allLoc = locators.get(parent_action).locator(placeholder).all();
		}
		else
		{
			allLoc = cur_page.locator(placeholder).all();
		}
		loc = resolveLocator(allLoc, payload);
		locators.put(actionName, loc);
	    if(payload.has("event"))
	    {
		    String event = payload.getString("event");
		    // another event loop goes here
		    if(event.equalsIgnoreCase("click"))
		    	loc.click();
	    }
	}

	public void getByRole(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		String placeholder = params.get(0).toString();
		String parent_action = null;
		Locator loc = null;
		List <Locator> allLoc = null;
		
		GetByRoleOptions options = createByRoleOptions(payload);
		// need to trap exceptions
		
		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			if(placeholder.equalsIgnoreCase("AriaRole.LIST"))
				allLoc = locators.get(parent_action).getByRole(AriaRole.LIST).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.BUTTON"))
				allLoc = locators.get(parent_action).getByRole(AriaRole.BUTTON).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.LINK"))
				allLoc = locators.get(parent_action).getByRole(AriaRole.LINK).all();
		}
		else
		{
			if(placeholder.equalsIgnoreCase("AriaRole.LIST"))
				allLoc = cur_page.getByRole(AriaRole.LIST, options).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.BUTTON"))
				allLoc = cur_page.getByRole(AriaRole.BUTTON, options).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.LINK"))
				allLoc = cur_page.getByRole(AriaRole.LINK, options).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.ROW"))
				allLoc = cur_page.getByRole(AriaRole.ROW, options).all();
		}
		loc = resolveLocator(allLoc, payload);
		locators.put(actionName, loc);
		
		final Locator newLoc = loc;
	    if(payload.has("event") && loc != null)
	    {
		    String event = payload.getString("event");
		    // another event loop goes here
	    	if(event.equalsIgnoreCase("click"))
	    	{
	    		if(!payload.has("new_page"))
	    		{
	    			loc.click();
	    		}
			    else
			    {
			    	// get the new page
			    	cur_page = cur_page.waitForPopup(() -> {
				        newLoc.click();
				      });
			    }
	    	}
	    }
	}

	public void mouse_xy(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		int x = Integer.parseInt(params.get(0).toString());
		int y = Integer.parseInt(params.get(1).toString());
		
		// move proportionally
		x = (cur_width / user_width)*x;
		y = (cur_height / user_height)*y;
		
		
		String event = "click";
		if(payload.has("event"))
		{
			event = payload.getString("event");
		}
		if(event.equalsIgnoreCase("click"))
		{
			ClickOptions op = new Mouse.ClickOptions();
			op.setButton(MouseButton.LEFT);
			if(payload.has("options"))
			{
				String option = payload.getString("options");
				if(option.equalsIgnoreCase("left"))
					op.setButton(MouseButton.LEFT);
				if(option.equalsIgnoreCase("right"))
					op.setButton(MouseButton.RIGHT);
				if(option.equalsIgnoreCase("middle"))
					op.setButton(MouseButton.MIDDLE);
			}
			// do we need to calculate some kind of bounded rectangle and click ? we need to see
			cur_page.mouse().click(x, y, op);
		}
		else if(event.equalsIgnoreCase("hover"))
		{	
			// introduce options later
			cur_page.mouse().move(x, y);
		}
	}
	
	public void keyboard(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		
		
		// phew this is going to be a deadly if then else
		String shift = payload.has(SHIFT) && payload.getBoolean(SHIFT) ? "Shift+":""; 
		String ctrl = payload.has(CTRL) && payload.getBoolean(CTRL) ? "ControlOrMeta+":"";
		
		// need to accomodate for this 
		// F1 - F12, Digit0- Digit9, KeyA- KeyZ, Backquote, Minus, Equal, Backslash, Backspace, Tab, Delete, Escape, ArrowDown, End, Enter, Home, Insert, PageDown, PageUp, ArrowRight, ArrowUp, etc.
		
		if(params.length() > 0)
		{
			this.cur_page.keyboard().down(shift);
			for (int paramIndex = 0;paramIndex < params.length();paramIndex++)
			{
				String keypress = params.getString(paramIndex);
				//keypress = shift + ctrl + keypress;
				this.cur_page.keyboard().press(keypress);
			}
			this.cur_page.keyboard().up(shift);
		}
	}
	
	public void keyboardPress(String input) {
		try {
			this.cur_page.keyboard().press(input);
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not press " + input, e);
		}
	}
	

	public void getByLabel(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		String placeholder = params.get(0).toString();
		placeholder = evalPlaceholder(placeholder);
		String parent_action = null;
		Locator loc = null;
		int sleep = 400;
		if(payload.has("sleep_after"))
			sleep = payload.getInt("sleep_after");

		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			loc = locators.get(parent_action).getByLabel(placeholder);
		}
		else
		{
			loc = cur_page.getByLabel(placeholder);
		}
		locators.put(actionName, loc);
		
	    if(payload.has("event"))
	    {
		    String event = payload.getString("event");
		    // another event loop goes here
		    if(event.equalsIgnoreCase("click"))
		    	loc.click();
		    if(event.equalsIgnoreCase("fill"))
		    {
		    	String type = payload.has("fill_type")?payload.getString("fill_type"):"string";
		    	if(type.equalsIgnoreCase("string"))
		    	{
		    		String value = payload.getString("fill_value");
		    		loc.fill(value);
		    	}
		    	else if(type.equalsIgnoreCase("int"))
		    	{
		    		int value = payload.getInt("fill_value");
		    		loc.fill(value + "");
		    	}
		    }
	    }
	}
	

	public void getByText(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		String placeholder = params.get(0).toString();
		placeholder = evalPlaceholder(placeholder);
		String parent_action = null;
		Locator loc = null;

		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			loc = locators.get(parent_action).getByText(placeholder);
		}
		else
		{
			loc = cur_page.getByText(placeholder);
		}
		locators.put(actionName, loc);
		
	    if(payload.has("event"))
	    {
		    String event = payload.getString("event");
		    // another event loop goes here
		    if(event.equalsIgnoreCase("click"))
		    	loc.click();
	    }
	}
	
	public boolean pause(JSONObject payload, String actionId)
	{
		boolean go = true;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String taskMessage = payload.getString("task_message");
			String actionMessage = payload.getString("action");
			System.err.println(taskMessage);
			System.err.println(actionMessage);
			String data = br.readLine();
			go =  data.toLowerCase().startsWith("y");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return go;
	}

	public void getUserInput(JSONObject payload, String actionId)
	{
		// get the user input and play it on the page
		// as you enter.. generate page from it
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		Locator loc = null;
		if(payload.has("parent_action"))
		{
			String parent_action = null;
			parent_action = payload.getString("parent_action");
			loc = locators.get(parent_action);
			boolean search = payload.has("search") && payload.getString("search").equalsIgnoreCase("true");
			String action = payload.getString("action");
			String data = null;
			boolean sync = payload.getString("synchronize").equalsIgnoreCase("true");
			if(loc != null)
			{
				do
				{
					String message = "Enter value";
					if(payload.has("message"))
						message = payload.getString("message");
					try {
						System.err.println(message);
						data = br.readLine();
						if(data.startsWith("!") && search) // this is a search we found our value come out of it
							break;
						else if(action.equalsIgnoreCase("fill") && !data.equalsIgnoreCase("e"))
						{
							
							loc.clear();
							loc.fill(data.replaceFirst("!", ""));
						}
						if(sync)
							trace(actionId, "- User - " + data);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}while(!data.startsWith("!"));
				
				if(payload.has("output"))
				{
					String outputName = payload.getString("output");
					variables.put(outputName, data.replace("!", ""));
				}
			}
		}
	}
	
	public void doFilter(JSONObject payload, String actionName)
	{
		JSONArray params = payload.getJSONArray("params");
		String placeholder = params.get(0).toString();
		
		// create filter options
		FilterOptions options = createFilterOptions(payload);
		
		String parent_action = null;
		Locator loc = null;
		List <Locator> allLoc = null;
		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			allLoc = locators.get(parent_action).filter(options).all();
		}
		loc = resolveLocator(allLoc, payload);
		
		locators.put(actionName, loc);
		
	    if(payload.has("event"))
	    {
		    String event = payload.getString("event");
		    // another event loop goes here
		    if(event.equalsIgnoreCase("click"))
		    	loc.click();
	    }
	}
	
	public String getScreenShot()
	{		
		String outputName = "a_" + Utility.getRandomString(8);
        try {
        	cur_page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get(outputDir + "/" + outputName + ".png")));
    		String filePath = Paths.get(outputDir + "/" + outputName + ".png").toString(); // Change this to the path of your image file
            File imageFile = new File(filePath);
            FileInputStream imageInFile = new FileInputStream(imageFile);

            // Reading the file's byte array
            byte[] imageData = new byte[(int) imageFile.length()];
            imageInFile.read(imageData);

            // Converting the byte array into Base64 string
            String base64Image = Base64.getEncoder().encodeToString(imageData);

            imageInFile.close();
            imageFile.delete();	
            return base64Image;
        } catch (IOException e) {
            System.out.println("Error while reading the file: " + e.getMessage());
        }
        return null;
	}
	
	public String getUrl()
	{		
		String url = null;
        try {
        	url = cur_page.url();
        } catch (Exception e) {
            System.out.println("Error while getting URL: " + e.getMessage());
        }
        return url;
	}
	
	public String getHTML()
	{		
		String html = null;
        try {
        	html = cur_page.content();
        } catch (Exception e) {
            System.out.println("Error while getting HTML: " + e.getMessage());
        }
        return html;
	}
	
	public void enterInput(String input) {
		cur_page.keyboard().type(input);
	}
	
	public Map getInputs()
	{
		// also do a sweep for all other input elements in general
        Document doc;
		Map idName = new HashMap();
		try {
			doc = Jsoup.parse(cur_page.content());
			
			// need to account for text box separately than the checkbox etc. etc. 
			Elements links = doc.select("input");
			makeObservable(links, "input", "onchange");
			
			// what format of output should I give
			// should it be name and id ?
			for(int linkIndex = 0;linkIndex< links.size();linkIndex++)
			{
				Element thisLink = links.get(linkIndex);
				String id = thisLink.attr(css_id);
				String name = thisLink.attr("name");
				idName.put(id, name);
			}
			
			// also do select
			Elements selects = doc.select("select");
			makeObservable(links, "select", "onchange");
			// these are drop down values
			for(int selectIndex = 0;selectIndex< selects.size();selectIndex++)
			{
				Element thisLink = selects.get(selectIndex);
				String id = thisLink.attr(css_id);
				String name = thisLink.attr("name");
				idName.put(id, name);
			}

			// handle text area
			Elements text = doc.select("textarea");
			for (int textIndex = 0; textIndex < text.size(); textIndex++) {
				Element thisLink = text.get(textIndex);
				String id = thisLink.attr(css_id);
				String name = thisLink.attr("name");
				idName.put(id, name);
			}
			
		}catch(Exception ex)
		{
			// ignoring
		}
		return idName;
	}
	
	public Map getChoices(String id)
	{
		// this will get the choice for that element
		Map retMap = new HashMap();
		if(inputLinks.containsKey(id))
		{
			Element elem = (Element)inputLinks.get(id);
			if(elem.tagName().equalsIgnoreCase("select"))
			{
				// get options for this select
				Elements options = elem.select("option");
				for(int optionIndex = 0;optionIndex < options.size();optionIndex++)
				{
					Element option = options.get(optionIndex);
					String key = option.attr("value");
					String value = option.text();
					retMap.put(key, value);
				}
			}
		}
		// for all other elements you will get an empty map you can change later
		
		
		return retMap;
	}
	
	public void fillInput(String id, Object val)
	{
		//select("option").val("value")
		if(inputLinks.containsKey(id))
		{
			Element elem = (Element)inputLinks.get(id);
			elem.val(val+"");
		}
	}

	private void downloadFile(JSONObject payload, String actionName)
	{
		if(payload.has("parent_action"))
		{
			String parent_action = payload.getString("parent_action");
			final Locator loc = locators.get(parent_action);
			Download download = cur_page.waitForDownload(() -> {
				loc.click();		
			});
			
			Path outputPath = Paths.get(payload.getString("path"));
			download.saveAs(outputPath);
		}
	}

	
	// private methods
	private GetByRoleOptions createByRoleOptions(JSONObject payload)
	{
		//page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Search")).click();
		GetByRoleOptions options = new Page.GetByRoleOptions();
		if(payload.has("options"))
		{
			JSONArray optionArray = payload.getJSONArray("options");
			JSONArray optionValues = payload.getJSONArray("option_values");
			for(int optionIndex = 0;optionIndex < optionArray.length();optionIndex++)
			{
				String optionName = optionArray.getString(optionIndex);
				String optionValue = optionValues.getString(optionIndex);
				if(optionName.equalsIgnoreCase("setName"))
					options.setName(optionValue);
			}
		}
		
		options.setExact(payload.has("options_exact") && payload.getString("options_exact").equalsIgnoreCase("True"));
		return options;
	}

	private FilterOptions createFilterOptions(JSONObject payload)
	{
		//page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Search")).click();
		FilterOptions options = new Locator.FilterOptions();
		if(payload.has("options"))
		{
			JSONArray optionArray = payload.getJSONArray("options");
			JSONArray optionValues = payload.getJSONArray("option_values");
			for(int optionIndex = 0;optionIndex < optionArray.length();optionIndex++)
			{
				String optionName = optionArray.getString(optionIndex);
				String optionValue = optionValues.getString(optionIndex);
				if(optionName.equalsIgnoreCase("setHasText"))
					options.setHasText(optionValue);
			}
		}		
		return options;
	}

	
	private Locator resolveLocator(List <Locator> allLoc, JSONObject payload)
	{
		// need also a try all option i.e. it tries one after the other.. 
		// we just dont know which one.. 
		
		// if there is only one it is easy
		Locator loc = null;
		if(allLoc.size() == 1)
			loc = allLoc.get(0);
		else if(payload.has("field_name"))
		{
			// need to see if there is some condition
			String field_name = payload.getString("field_name");
			String comparator = payload.getString("field_comparator");
			String field_type = "string" ; //others we will incorporate later
			String field_value = payload.getString("field_value");
			for(int locIndex = 0;locIndex < allLoc.size();locIndex++)
			{
				Locator thisLocator = allLoc.get(locIndex);
				String thisFieldValue = thisLocator.getAttribute(field_name);
				if(comparator.equalsIgnoreCase("=="))
				{
					if(thisFieldValue.contains(field_value))
					{
						loc = thisLocator;
						break;
					}
				}
				else if(comparator.equalsIgnoreCase("!="))
				{
					if(thisFieldValue.contains(field_value))
					{
						loc = thisLocator;
						break;
					}					
				}
			}
		}
		else if(payload.has("ordinal"))
		{
			int ordinal = payload.getInt("ordinal");
			loc = allLoc.get(ordinal);
		}
		else if(allLoc.size() > 0)
			loc = allLoc.get(0);
		
		// super navigation i.e. do we need the parent.. if so do that
		if(payload.has("super_navigate") && loc != null)
		{
			int navLevels = payload.getInt("super_navigate");
			for(int navIndex = 0;navIndex < navLevels;navIndex++)
				loc = loc.locator("..");
		}
		
		return loc;
	}
	
	
	private String evalPlaceholder(String placeholder)
	{
		if(placeholder.startsWith("_") && variables.containsKey(placeholder))
			return variables.get(placeholder);
		return placeholder;
	}
	
	
	// does the prints of page etc. 	
	private void trace(String actionId, String actionName)
	{
		boolean success = false;
		int attempt = 3;
		inputLinks = new Hashtable();
		while(!success)
		{
			try {
				Thread.sleep(sleep);
				String outputName = this.session + actionId;
				if(this.tracePage)
				{
					// output html
					String htmlContent = cur_page.content();			
					htmlContent = convertRelativeToAbsoluteLinks(htmlContent, htmlContent);
					FileUtils.write(new File(outputDir + "/" + outputName + ".html"), htmlContent, Charset.defaultCharset());
				}
				if(this.traceScreenshot)
				{
					cur_page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get(outputDir + "/" + outputName + ".png")));
				}
				success = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch(PlaywrightException ex)
			{
				String message = ex.getMessage();
				if(message.contains("navigating") && attempt < 4)
					attempt++;
				else success = true;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public String convertRelativeToAbsoluteLinks(String baseUrl, String page)
	{
        // Base URL of the document (you might need to adjust this based on your actual use case)

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
			//makeObservable(links, "button", "onclick");
			
			links = doc.select("a");
			//makeObservable(links, "hyper", "onclick");
			
			// put all the inputs
			// I think we need to capture the forms also
			List <FormElement> forms = doc.forms();
			for(int formIndex = 0;formIndex < forms.size();formIndex++)
			{
				// get all the input elements first
				// then get the button.. the button is what we need to click once the user fills basically - dont need the button since it will be a click event
				FormElement thisForm = forms.get(formIndex);
				links = thisForm.select("input");
				List inputLinkList = makeObservable(links, "input", "onchange");
				formInputs.put(thisForm, inputLinkList);
			}	
			// also do a sweep for all other input elements in general
			links = doc.select("input");
			makeObservable(links, "input", "onchange");
			
			// also capture all the links / elements
			//printElements(links);
			links = doc.select("button");
			
			// Convert relative links to absolute
			return doc.html();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Print the updated HTML document
        //System.out.println(doc.html());
		return null;
	}
	
	private List makeObservable(Elements links, String name, String event)
	{
		List inputLinkList = new ArrayList<Element>();
		for(int linkIndex = 0;linkIndex < links.size();linkIndex++)
		{
			Element link = links.get(linkIndex);
			link.attr(css_id, name+linkIndex);
			//link.attr(event, "alert(document.querySelector('[" + css_id + "=" + name + linkIndex + "]').value)");
			link.removeAttr("disabled");
			String id = link.attr(css_id);
			inputLinks.put(id, link);	
			inputList.add(id);
			inputLinkList.add(link);
			
			// get the id and name also
			if(link.hasAttr("id"))
				inputLinks.put(link.attr("id"), link);
			if(link.hasAttr("name"))
				inputLinks.put(link.attr("name"), link);
		}
		return inputLinkList;
	}
	
	private void replacer(String baseUrl, Elements links, String attr)
	{
		try {
			for (Element link : links) {
			    String href = link.attr(attr);
			    //System.err.println("Old Link " + href);
			    if (href.startsWith("/") || href.startsWith("./") || href.startsWith("../") || !href.startsWith("http")) 
			    {
			        URI baseUri = new URI(baseUrl);
			        URI linkUri = new URI(href);
			        URI absoluteUri = baseUri.resolve(linkUri);
			        link.attr(attr, absoluteUri.toString());
				    //System.err.println("New Link " + absoluteUri.toString());
			    }
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		
	}

	private void handleSleep(JSONObject payload)
	{
		if(payload.has("sleep_after"))
		{
			int sleepTime = payload.getInt("sleep_after");
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
	

	
	//Las-Vegas-McCarran-LAS
	//Washington, D.C., District of
	//Los Angeles, California,
	
	
	/*
	 *  Save and submit - getByLabel("Save & Submit")
  getByLabel("Open Comments section")
  getByLabel("Expand or Collapse Reasons").first()
    getByText("Additional Time Worked")
    getByPlaceholder("Enter text")
    
    page.getByLabel("Add", new Page.GetByLabelOptions().setExact(true)).click();
    
	 */

