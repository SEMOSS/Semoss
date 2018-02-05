package prerna.sablecc2.reactor.utils;

import java.util.TreeSet;

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
	
	
	@Override
	public NounMetadata execute() {
		
		// create a string builder to keep track of all categories of reactors
		StringBuilder allReactors = new StringBuilder();
		
		// general reactors
		TreeSet general = new TreeSet(ReactorFactory.reactorHash.keySet());
		if (general.size() > 0) {
			allReactors.append("General Reactors: \n").append(formatOutput(general));
		}
		
		// the frame specific reactors
		//rframe
		TreeSet rFrame = new TreeSet(ReactorFactory.rFrameHash.keySet());
		if (rFrame.size() > 0) {
			allReactors.append("R Frame Reactors: \n").append(formatOutput(rFrame));
		}
		
		//h2
		TreeSet h2Frame = new TreeSet(ReactorFactory.h2FrameHash.keySet());
		if (h2Frame.size() > 0) {
			allReactors.append("H2 Frame Reactors: \n").append(formatOutput(h2Frame));
		}
		
		//native
		TreeSet nativeFrame = new TreeSet(ReactorFactory.nativeFrameHash.keySet());
		if (nativeFrame.size() > 0) {
			allReactors.append("Native Frame Reactors: \n").append(formatOutput(nativeFrame));
		}
		
		//tinker
		TreeSet tinkerFrame = new TreeSet(ReactorFactory.tinkerFrameHash.keySet());
		if (tinkerFrame.size() > 0) {
			allReactors.append("Tinker Frame Reactors: \n").append(formatOutput(tinkerFrame));
		}
		
		// the expression set
		TreeSet expressionSet = new TreeSet(ReactorFactory.expressionHash.keySet());
		if (expressionSet.size() > 0) {
			allReactors.append("Expression Set Reactors: \n").append(formatOutput(expressionSet));
		}
		
		//return a string with all of the reactors
		String reactors = allReactors.toString();
		return new NounMetadata(reactors, PixelDataType.CONST_STRING, PixelOperationType.HELP);
		
	}
	
	private String formatOutput(TreeSet t) {
		//keep track of the output using a string builder
		StringBuilder formatString = new StringBuilder();
		//use a count so that we can go to a new line every three entries (create three columns)
		int count = 0;
		//iterate through the values of the tree map and include spaces between them
		for (Object value : t) {
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
