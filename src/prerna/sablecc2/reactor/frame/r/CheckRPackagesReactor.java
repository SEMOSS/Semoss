package prerna.sablecc2.reactor.frame.r;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckRPackagesReactor extends AbstractRFrameReactor {
	
	//CheckRPackages(widget=["test"])
	private static final String WIDGET = "widget";
	
	public CheckRPackagesReactor() {
		this.keysToGet = new String[]{WIDGET};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		
		//get intput
		String widgetName = getWidgetName();
		List<String> rPackages = new ArrayList<String>();
		switch(widgetName) {
		case "analytics-associated-learning":
			rPackages.addAll(Arrays.asList("arules"));
			break;
		case "analytics-clustering":
		case "analytics-multi-clustering":
			rPackages.addAll(Arrays.asList("cluster", "stats"));
			break;
		case "analytics-classification":
			rPackages.addAll(Arrays.asList("partykit"));
			break;
		case "analytics-lof":
			rPackages.addAll(Arrays.asList("Rlof", "VGAM"));
			break;
		case "analytics-random-forest":
			rPackages.addAll(Arrays.asList("randomForest"));
			break;
		}
		
		//check if r packages are installed
		this.rJavaTranslator.checkPackages(rPackages.stream().toArray(String[]::new));
		
		return null;
	}
	
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods//////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getWidgetName() {
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		if (grs != null) {
			return (String) grs.getNoun(0).getValue();
		} else {
			throw new IllegalArgumentException("The widget must be specified to check for relevant R packages.");
		}
	}

}
