package prerna.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.jsoup.select.Elements;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Download;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Locator.FilterOptions;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Page.GetByRoleOptions;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.PlaywrightException;
import com.microsoft.playwright.options.AriaRole;


public class Playwrighter2 {
	
	Playwright playwright = Playwright.create();
	public static final String css_id = "pw_id";
	List inputList = new ArrayList();
	Map inputLinks = new Hashtable();
	Map <String, Locator> locators = new HashMap();
	Map <String, String> variables = new HashMap();
	String outputDir = "c:/temp/playwright";
	String session = null;
	String baseUrl = null;
	int sleep = 400;
	
	
	boolean tracePage = false;
	boolean traceScreenshot = false;
	
	Page cur_page = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// read the file
		// for every action do the appropriate action
		String fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/playwright.json";
		fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/timesheet.json";
		fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/downloader.json";
		fileName = "c:/users/pkapaleeswaran/workspacegit/playwright/timesheet_time_approval.json";
		
		Playwrighter2 pw = new Playwrighter2();
		pw.initPlaywright();
		pw.tracePage = true;
		pw.traceScreenshot = true;
		pw.processFile(fileName);
	}

	public void initPlaywright()
	{
	     //LaunchOptions lp = new LaunchOptions();
	     //lp.setChannel(BrowserChannel.CHROME);
	     //lp.setHeadless(false);
	     
	     BrowserType firefox = playwright.chromium();
	     //BrowserType firefox = playwright.webkit();
	     Browser browser = firefox.launch();
	     BrowserContext ctx = browser.newContext();
	     //page = ctx.newPage();
	     cur_page = ctx.newPage();
	     session = java.util.UUID.randomUUID() +"";
	     
	     // default sleep value to 200
	     this.sleep = 200;

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
				boolean each = action.has("each") && action.getString("each").equalsIgnoreCase("true");
				if(each)
				{
					JSONArray params = action.getJSONArray("params");
					for(int paramIndex = 0;paramIndex < params.length();paramIndex++)
					{
						JSONArray arr = new JSONArray();
						arr.put(params.getString(paramIndex));
						action.put("params", arr);
						System.err.println("Processing EACH >>>> " + params.getString(paramIndex)+ "<<<<<<");
						go = processAction(action, actionId);
					}
				}
				else
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
		int retry = 1; // default is execute atleast once
		if(critical && action.has("retry"))
		{
			retry = action.getInt("retry");
			
		}
		int attempt = 0;
		while(attempt < retry)
		{
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
			attempt++;
		}
		
		if(action.has("debug") && action.getString("debug").equalsIgnoreCase("true") && locators.containsKey(actionId))
			System.err.println(locators.get(actionId).innerHTML());
		
		return go;
	}
	
	
	public void navigate(JSONObject payload)
	{
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
		    if(event.equalsIgnoreCase("hover"))
		    	loc.hover();
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
		com.microsoft.playwright.Locator.GetByRoleOptions options2 = createByRoleOptions2(payload);
		// need to trap exceptions
		
		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			if(placeholder.equalsIgnoreCase("AriaRole.LIST"))
				allLoc = locators.get(parent_action).getByRole(AriaRole.LIST, options2).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.BUTTON"))
				allLoc = locators.get(parent_action).getByRole(AriaRole.BUTTON, options2).all();
			else if(placeholder.equalsIgnoreCase("AriaRole.LINK"))
				allLoc = locators.get(parent_action).getByRole(AriaRole.LINK, options2).all();
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
	    	else if(event.equalsIgnoreCase("hover"))
	    	{
	    		loc.hover();
	    	}
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
		List <Locator> allLoc = null;
		Locator loc = null;

		if(payload.has("parent_action"))
		{
			parent_action = payload.getString("parent_action");
			allLoc = locators.get(parent_action).getByText(placeholder).all();
		}
		else
		{
			allLoc = cur_page.getByText(placeholder).all();
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

	private com.microsoft.playwright.Locator.GetByRoleOptions createByRoleOptions2(JSONObject payload)
	{
		//page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Search")).click();
		com.microsoft.playwright.Locator.GetByRoleOptions options = new com.microsoft.playwright.Locator.GetByRoleOptions();
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
			makeObservable(links, "button", "onclick");
			
			links = doc.select("a");
			//makeObservable(links, "hyper", "onclick");
			
			// put all the inputs
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
	
	private void makeObservable(Elements links, String name, String event)
	{
		
		for(int linkIndex = 0;linkIndex < links.size();linkIndex++)
		{
			Element link = links.get(linkIndex);
			link.attr(css_id, name+linkIndex);
			link.attr(event, "alert(document.querySelector('[" + css_id + "=" + name + linkIndex + "]').value)");
			link.removeAttr("disabled");
			String id = link.attr(css_id);
			inputLinks.put(id, link);	
			inputList.add(id);
			
			// get the id and name also
			if(link.hasAttr("id"))
				inputLinks.put(link.attr("id"), link);
			if(link.hasAttr("name"))
				inputLinks.put(link.attr("name"), link);
		}
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
}
