package prerna.sablecc2.reactor.utils;

import java.util.TreeSet;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ReactorFactory;

public class HelpReactor extends AbstractReactor {
	
	/**
	 * This reactor allows the user to view the names of all reactors
	 * There are no inputs to the reactor
	 */
	
	private static final String RESET_KEY = "reset";
	private static String helpString = null;
	private static String adminHelpString = null;

	public HelpReactor() {
		this.keysToGet = new String[] {RESET_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		boolean isAdmin = (SecurityAdminUtils.getInstance(user)) != null;
		organizeKeys();
		boolean reset = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[0]));
		if(isAdmin) {
			if(reset || adminHelpString == null) {
				HelpReactor.adminHelpString = createHelpString(true);
			}
			return new NounMetadata(HelpReactor.adminHelpString, PixelDataType.CONST_STRING, PixelOperationType.HELP);
		} else {
			if(reset || helpString == null) {
				HelpReactor.helpString = createHelpString(false);
			}
			return new NounMetadata(HelpReactor.helpString, PixelDataType.CONST_STRING, PixelOperationType.HELP);
		}
	}
	
	/**
	 * 
	 * @param isAdmin
	 * @return
	 */
	private String createHelpString(boolean isAdmin) {
		// create a string builder to keep track of all categories of reactors
		StringBuilder allReactors = new StringBuilder();
		
		// general reactors
		TreeSet general = new TreeSet(ReactorFactory.reactorHash.keySet());
		if (general.size() > 0) {
			allReactors.append("General Reactors: \n").append(formatOutput(general, isAdmin));
		}
		
		// the frame specific reactors
		//rframe
		TreeSet rFrame = new TreeSet(ReactorFactory.rFrameHash.keySet());
		if (rFrame.size() > 0) {
			allReactors.append("R Frame Reactors: \n").append(formatOutput(rFrame, isAdmin));
		}
		
		//h2
		TreeSet h2Frame = new TreeSet(ReactorFactory.h2FrameHash.keySet());
		if (h2Frame.size() > 0) {
			allReactors.append("H2 Frame Reactors: \n").append(formatOutput(h2Frame, isAdmin));
		}
		
		//native
		TreeSet nativeFrame = new TreeSet(ReactorFactory.nativeFrameHash.keySet());
		if (nativeFrame.size() > 0) {
			allReactors.append("Native Frame Reactors: \n").append(formatOutput(nativeFrame, isAdmin));
		}
		
		//tinker
		TreeSet tinkerFrame = new TreeSet(ReactorFactory.tinkerFrameHash.keySet());
		if (tinkerFrame.size() > 0) {
			allReactors.append("Tinker Frame Reactors: \n").append(formatOutput(tinkerFrame, isAdmin));
		}
		
		// the expression set
		TreeSet expressionSet = new TreeSet(ReactorFactory.expressionHash.keySet());
		if (expressionSet.size() > 0) {
			allReactors.append("Expression Set Reactors: \n").append(formatOutput(expressionSet, isAdmin));
		}
		
		return allReactors.toString();
	}
	
	private String formatOutput(TreeSet t, boolean isAdmin) {
		//keep track of the output using a string builder
		StringBuilder formatString = new StringBuilder();
		//use a count so that we can go to a new line every three entries (create three columns)
		int count = 0;
		//iterate through the values of the tree map and include spaces between them
		for (Object value : t) {
			// if not an admin
			// dont show Admin reactors as options
			if(!isAdmin) {
				if(value.toString().toLowerCase().startsWith("admin")) {
					continue;
				}
			}
			formatString.append(value);
			//keep track of the amount of spaces to add
			//add enough to line up the columns
			String spaces = " ";
			if (value.toString().length() < 35) {
				for (int i = value.toString().length(); i < 35; i++) {
					spaces += " ";
				}
			}
			formatString.append(spaces);
			count++;
			//go to a new line
			if (count == 3) {
				formatString.append("\n");
				count = 0;
			}
		}
		formatString.append("\n\n");
		return formatString.toString();
	}

}
