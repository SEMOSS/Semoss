package prerna.reactor.playwright;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;


public class ReplayStepReactor extends AbstractReactor {
	
	public static Path recordingsDir = initRecordingsDir();
    static ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    static Insight insightObj;
	Browser browser;
	Map<String, Object> response = new HashMap<>();
	
	public ReplayStepReactor(){
		this.keysToGet = new String[] {
				"sessionId",
				"fileName",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), //inputs
				"executeAll" //Execute all actions in current page
				};
		this.keyRequired = new int[] { 1,1,0,0 };
		insightObj = this.insight;
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
    	Map<String, Object> inputs = Utility.getMap(this.store, this.curRow);
		String name = this.keyValue.get(this.keysToGet[1]);
		ScreenshotResponse screenshot = replayFromFile(inputs, name);
		response.put("screenshot", screenshot);
		
        return new NounMetadata(response, PixelDataType.MAP);
	}
	
    public ScreenshotResponse replayFromFile(Map<String, Object> inputs, String nameOrPath) {
        StepsEnvelope env = loadStepsFromFile(nameOrPath);
        return replay(env, inputs);
    }
	
	public ScreenshotResponse replay(StepsEnvelope steps, Map<String, Object> inputs) {
		boolean executeAll = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]));
		List<List<Step>> allStepsList = steps.steps();
		Playwright pw = Playwright.create();
		browser = pw.chromium().launch(
	            new BrowserType.LaunchOptions().setHeadless(true));
        BrowserContext ctx = browser.newContext(new Browser.NewContextOptions()
                .setViewportSize(allStepsList.get(0).get(0).viewport().width(),
                		allStepsList.get(0).get(0).viewport().height())
                .setDeviceScaleFactor(allStepsList.get(0).get(0).viewport().deviceScaleFactor())
        );
        Page page = ctx.newPage();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
        Session s = SessionReactor.get(sessionId);
        
        if(s.currentPageIndex == 0) {
            StepReactor.applyStep(s, allStepsList.get(0).get(0));
            s.currentPageIndex++;
        } else {
        	if(executeAll) {
        		for (; s.currentStepIndex<allStepsList.get(s.currentPageIndex).size();s.currentStepIndex++) {
        			Step step = allStepsList.get(s.currentPageIndex).get(s.currentStepIndex);
        			if (step.type() == StepType.TYPE && inputs.containsKey(step.label())) {
        				Step newStep =  new Step (step, inputs.get(step.label()).toString());
            			StepReactor.applyStep(s, newStep);
        			} else {
        				StepReactor.applyStep(s, step);
        			}
        		}
        		s.currentPageIndex++;
        		s.currentStepIndex = 0;
        	} else {
    			Step step = allStepsList.get(s.currentPageIndex).get(s.currentStepIndex);
        		if(inputs == null || inputs.isEmpty()) {
    				StepReactor.applyStep(s, step);
    				s.currentStepIndex++;
        		} else {
        			if (step.type() == StepType.TYPE && inputs.containsKey(step.label())) {
        				Step newStep =  new Step (step, inputs.get(step.label()).toString());
            			StepReactor.applyStep(s, newStep);
        			} else {
            			StepReactor.applyStep(s, step);
        			}
        			s.currentStepIndex++;
        		}
        	}
    	}
        s.isLastPage = allStepsList.size()-1 == s.currentPageIndex && s.currentStepIndex == allStepsList.get(s.currentPageIndex).size();
        
        if(!s.isLastPage) {
        	if (s.currentStepIndex  >= allStepsList.get(s.currentPageIndex).size()) {
        		s.currentStepIndex = 0;
    			s.currentPageIndex++;
        	}
		}
        if(s.currentPageIndex < allStepsList.size()) {
        	response.put("actions", getPageActions(allStepsList.get(s.currentPageIndex), s.currentStepIndex));
        }
        response.put("isLastPage", s.isLastPage);
        return ScreenshotReactor.screenshot(sessionId);
    }
	
    public static StepsEnvelope loadStepsFromFile(String nameOrPath) {
        Path file = nameOrPath.contains(FileSystems.getDefault().getSeparator())
                ? Paths.get(nameOrPath)
                : recordingsDir.resolve(nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json");

        try {
            return json.readValue(file.toFile(), StepsEnvelope.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read: " + file, e);
        }
    }
	
    public List<String> listRecordings() {
        try (var stream = Files.list(recordingsDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list recordings", e);
        }
    }
	
	public static Path initRecordingsDir() {
        try {
            
//            Path dir = Path.of(AssetUtility.getProjectAssetsFolder(insightObj.getContextProjectName(), insightObj.getContextProjectId()), "recordings");
        	Path dir = Path.of("C:/workspace/Apps/recordings");

            Files.createDirectories(dir); // creates recordings folder
            return dir;
        } catch (Exception ex) {
            throw new RuntimeException("Cannot create recordings dir", ex);
        }
    }
	
	private List<Map<String, Object>> getPageActions(List<Step> steps, int currentStepIndex) {
		List<Map<String, Object>> actionsList = new ArrayList<>();
		for(int i=currentStepIndex; i<steps.size();i++) {
			Map<String, Object> action = new HashMap<>();
			Step current = steps.get(i);
			switch (current.type()) {
			case TYPE:
				action.put("TYPE", new VariableRecord(current.label(), current.text(), current.isPassword()));
				break;
			case CLICK:
				action.put("CLICK", current.coords());
				break;
			case SCROLL:
				action.put("SCROLL", current.deltaY());
				break;
			case NAVIGATE:
				action.put("NAVIGATE", current.url());
				break;
			case WAIT: 
				action.put("WAIT", current.waitAfterMs());
			default:
				break;
			}
			actionsList.add(action);
		}
		return actionsList;
	}

}
