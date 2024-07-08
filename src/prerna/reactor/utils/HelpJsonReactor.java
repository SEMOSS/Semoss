package prerna.reactor.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class HelpJsonReactor extends AbstractReactor {
	
	/**
	 * This reactor allows the user to view the names of all reactors
	 * There are no inputs to the reactor
	 */
	
	private static final String RESET_KEY = "reset";
	private static Map<String, Set<String>> helpMap = null;
	private static Map<String, Set<String>> adminHelpMap = null;

	public HelpJsonReactor() {
		this.keysToGet = new String[] {RESET_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		boolean isAdmin = (SecurityAdminUtils.getInstance(user)) != null;
		organizeKeys();
		boolean reset = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[0]));
		if(isAdmin) {
			if(reset || adminHelpMap == null) {
				HelpJsonReactor.adminHelpMap = createHelp(true);
			}
			return new NounMetadata(HelpJsonReactor.adminHelpMap, PixelDataType.MAP, PixelOperationType.HELP_JSON);
		} else {
			if(reset || helpMap == null) {
				HelpJsonReactor.helpMap = createHelp(false);
			}
			return new NounMetadata(HelpJsonReactor.helpMap, PixelDataType.MAP, PixelOperationType.HELP_JSON);
		}
	}
	
	/**
	 * 
	 * @param isAdmin
	 * @return
	 */
	private Map<String, Set<String>> createHelp(boolean isAdmin) {
		Map<String, Set<String>> retMap = new HashMap<>();
		retMap.put("General", nonAdminFormat(new TreeSet(ReactorFactory.reactorHash.keySet()), isAdmin));
		retMap.put("R", nonAdminFormat(new TreeSet(ReactorFactory.rFrameHash.keySet()), isAdmin));
		retMap.put("PYTHON", nonAdminFormat(new TreeSet(ReactorFactory.pandasFrameHash.keySet()), isAdmin));
		retMap.put("H2", nonAdminFormat(new TreeSet(ReactorFactory.h2FrameHash.keySet()), isAdmin));
		retMap.put("NATIVE", nonAdminFormat(new TreeSet(ReactorFactory.nativeFrameHash.keySet()), isAdmin));
		retMap.put("TINKER", nonAdminFormat(new TreeSet(ReactorFactory.tinkerFrameHash.keySet()), isAdmin));
		retMap.put("EXPRESSION", nonAdminFormat(new TreeSet(ReactorFactory.expressionHash.keySet()), isAdmin));
		return retMap;
	}
	
	private Set<String> nonAdminFormat(TreeSet<String> t, boolean isAdmin) {
		if(isAdmin) {
			return t;
		}
		Iterator<String> iterator = t.iterator();
		while(iterator.hasNext()) {
			if(iterator.next().toString().toLowerCase().startsWith("admin")) {
				iterator.remove();
			}
		}
		return t;
	}
	
}
