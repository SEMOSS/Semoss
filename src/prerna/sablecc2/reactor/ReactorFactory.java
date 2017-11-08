package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.reactor.qs.QueryStructReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ReactorFactory {

	// This holds the reactors that are frame agnostic and can be used by pixel
	private static Map<String, Class> reactorHash;

	// This holds the reactors that are expressions
	// example Sum, Max, Min
	// the reactors will handle how to execute
	// if it can be run via the frame (i.e. sql/gremlin) or needs to run external
	private static Map<String, Class> expressionHash;

	// this holds that base package name for frame specific reactors
	private static Map<String, Class> rFrameHash;
	private static Map<String, Class> h2FrameHash;
	private static Map<String, Class> tinkerFrameHash;
	private static Map<String, Class> nativeFrameHash;
	
	static {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		reactorHash = new HashMap<>();
		// build generic reactor hash
		buildReactorHashFromPropertyFile(reactorHash, baseFolder + "\\src\\reactors.prop");

		expressionHash = new HashMap<>();
		// build expression hash
		buildReactorHashFromPropertyFile(expressionHash, baseFolder + "\\src\\expressionSetReactors.prop");

		rFrameHash = new HashMap<>();
		h2FrameHash = new HashMap<>();
		tinkerFrameHash = new HashMap<>();
		nativeFrameHash = new HashMap<>();
		// populate the frame specific hashes
		buildReactorHashFromPropertyFile(rFrameHash, baseFolder + "\\src\\rFrameReactors.prop");
		buildReactorHashFromPropertyFile(h2FrameHash, baseFolder + "\\src\\h2FrameReactors.prop");
		buildReactorHashFromPropertyFile(tinkerFrameHash, baseFolder + "\\src\\tinkerFrameReactors.prop");
		buildReactorHashFromPropertyFile(nativeFrameHash, baseFolder + "\\src\\nativeFrameReactors.prop");
	}

	/**
	 * 
	 * @param reactorId
	 *            - reactor name
	 * @param nodeString
	 *            - pixel
	 * @param frame
	 *            - frame we will be operating on
	 * @param parentReactor
	 *            - the parent reactor
	 * @return
	 * 
	 * 		This will simply return the IReactor responsible for execution
	 *         based on the reactorId
	 * 
	 *         Special case: if we are dealing with an expression, we determine
	 *         if this expression is part of a select query or should be reduced
	 *         If it is a reducing expression we 1. create an expr reactor 2.
	 *         grab the reducing expression reactor from the frame 3. set that
	 *         reactor to the expr reactor and return the expr reactor The expr
	 *         reactor when executed will use that reducing expression reactor
	 *         to evaluate
	 */
	public static IReactor getReactor(String reactorId, String nodeString, ITableDataFrame frame, IReactor parentReactor) {
		IReactor reactor = null;
		try {
			// is this an expression?
			// we need to determine if we are treating this expression as a
			// reducer or as a selector
			if (expressionHash.containsKey(reactorId.toUpperCase())) {
				// if this expression is not a selector
				if (!(parentReactor instanceof QueryStructReactor)) {
					reactor = (IReactor) expressionHash.get(reactorId.toUpperCase()).newInstance();
					reactor.setPixel(reactorId, nodeString);
					return reactor;
				}
			}
			
			// see if it is a frame specific reactor
			if (frame != null) {
				// identify the correct hash to use
				if (frame instanceof H2Frame) {
					// see if the hash contains the reactor id
					if (h2FrameHash.containsKey(reactorId)) {
						reactor = (IReactor) h2FrameHash.get(reactorId).newInstance();
					}
				} else if (frame instanceof RDataTable) {
					if (rFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) rFrameHash.get(reactorId).newInstance();
					}
				} else if (frame instanceof TinkerFrame) {
					if (tinkerFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) tinkerFrameHash.get(reactorId).newInstance();
					}
				} else if (frame instanceof NativeFrame) {
					if (nativeFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) nativeFrameHash.get(reactorId).newInstance();
					}
				} else {
					throw new IllegalArgumentException("Frame type not supported");
				}

				// if we have retrieved a reactor from a frame hash
				if (reactor != null) {
					reactor.setPixel(reactorId, nodeString);
					return reactor;
				}
			}
			
			// see if it is a generic one
			// if not an expression
			// search in the normal reactor hash
			if (reactorHash.containsKey(reactorId)) {
				reactor = (IReactor) reactorHash.get(reactorId).newInstance();
				reactor.setPixel(reactorId, nodeString);
				return reactor;
			}
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		
		// ughhh... idk what you are trying to do
		// reactor = new SamplReactor();
		throw new IllegalArgumentException("Cannot find reactor for keyword = " + reactorId);
	}

	public static boolean hasReactor(String reactorId) {
		return reactorHash.containsKey(reactorId) || expressionHash.containsKey(reactorId.toUpperCase());
	} 
	
	/**
	 * This method takes in a prop file to build the reactorHash
	 * 
	 * @param propFile
	 *            - the path to the prop file with the reactor names and classes
	 * @param reactorHash
	 *            - the specific reactor hash object that we are building
	 * 
	 */
	public static void buildReactorHashFromPropertyFile(Map<String, Class> hash, String propFile) {
		// move info from the prop file into a Properties object
		Properties properties = Utility.loadProperties(propFile);
		// for each line in the file
		// each line maps a reactor (operation) to a class
		for (Object operation : properties.keySet()) {
			try {
				// identify the class that corresponds to each reactor
				String reactorClass = properties.get(operation).toString();
				Class reactor = (Class.forName(reactorClass));
				// put the operation and the class into the reactor hash
				hash.put(operation.toString(), reactor);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
