package prerna.reactor.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Properties;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddOperationAliasReactor extends AbstractReactor {

	/**
	 * This reactor takes in the name of a reactor and allows the user to assign
	 * it an alias The inputs to the reactor are: 
	 * 1) the name of the reactor 
	 * 2) the alias for the reactor
	 */
	
	public AddOperationAliasReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.REACTOR.getKey(), ReactorKeysEnum.ALIAS.getKey()};
	}

	@Override
	public NounMetadata execute() {
//
//		// get inputs
//		String reactorName = getReactorName();
//		String alias = getReactorAlias();
//
//		// logic for this reactor:
//		// first: check the regular reactor hash to see if the reactor is specified there
//		// if it is, then update that prop file
//		// then check the expression hash
//		// then check the frame-specific hashes
//		// use a counter to make sure that at least one file was updated
//		// if nothing is updated then throw an error
//		int counter = 0;
//
//		// for each properties file:
//		// get the properties object, if the reactor can be found in the properties object
//		// if it can be found, add the alias to the properties object
//		// then update the file
//
//		// general reactor hash
//		Properties prop = addReactorAliasToProperties(ReactorFactory.REACTOR_PROP_PATH, reactorName, alias);
//		if (prop != null && prop.size() > 0) {
//			updatePropFile(prop, ReactorFactory.REACTOR_PROP_PATH);
//			counter++;
//		}
//
//		// expression hash
//		prop = addReactorAliasToProperties(ReactorFactory.EXPRESSION_PROP_PATH, reactorName, alias);
//		if (prop != null && prop.size() > 0) {
//			updatePropFile(prop, ReactorFactory.EXPRESSION_PROP_PATH);
//			counter++;
//		}
//
//		// frame specific hashes
//		if (counter < 1) {
//			prop = addReactorAliasToProperties(ReactorFactory.H2_FRAME_PROP_PATH, reactorName, alias);
//			if (prop != null && prop.size() > 0) {
//				updatePropFile(prop, ReactorFactory.H2_FRAME_PROP_PATH);
//				counter++;
//			}
//
//			prop = addReactorAliasToProperties(ReactorFactory.R_FRAME_PROP_PATH, reactorName, alias);
//			if (prop != null && prop.size() > 0) {
//				updatePropFile(prop, ReactorFactory.R_FRAME_PROP_PATH);
//				counter++;
//			}
//
//			prop = addReactorAliasToProperties(ReactorFactory.NATIVE_FRAME_PROP_PATH, reactorName, alias);
//			if (prop != null && prop.size() > 0) {
//				updatePropFile(prop, ReactorFactory.NATIVE_FRAME_PROP_PATH);
//				counter++;
//			}
//
//			prop = addReactorAliasToProperties(ReactorFactory.TINKER_FRAME_PROP_PATH, reactorName, alias);
//			if (prop != null && prop.size() > 0) {
//				updatePropFile(prop, ReactorFactory.TINKER_FRAME_PROP_PATH);
//				counter++;
//			}
//		}
//
//		// return metadata if one of the prop files has been updated
//		if (counter > 0) {
//			boolean fileUpdated = true;
//			return new NounMetadata(fileUpdated, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
//		} else {
//			throw new IllegalArgumentException("The specified reactor cannot be found");
//		}
		return null;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getReactorName() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata reactorNoun = inputsGRS.getNoun(0);
			return reactorNoun.getValue() + "";
		}
		throw new IllegalArgumentException("Need to specify the reactor to create an alias for");
	}

	private String getReactorAlias() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS.size() > 1) {
			NounMetadata aliasNoun = inputsGRS.getNoun(1);
			return aliasNoun.getValue() + "";
		}
		throw new IllegalArgumentException("Need to specify the alias");
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// UPDATE PROP FILE ///////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	// this method will create a properties object and see if the reactor is
	// found there
	// if it is found, we add the alias and return the updated properties object
	private Properties addReactorAliasToProperties(String path, String reactorName, String alias) {
		Properties properties = Utility.loadProperties(path);
		try {
			// identify the class that corresponds to each reactor
			if (properties.get(reactorName) != null) {
				String reactorClass = properties.get(reactorName) + "";
				Class reactor = (Class.forName(reactorClass));
				// add alias to the prop file
				properties.put(alias, reactor.getName());
				// also update the appropriate hash so that we do not have to
				// restart the server every time we add an alias
//				if (path.equals(ReactorFactory.REACTOR_PROP_PATH)) {
//					ReactorFactory.reactorHash.put(alias, reactor);
//				} else if (path.equals(ReactorFactory.EXPRESSION_PROP_PATH)) {
//					ReactorFactory.expressionHash.put(alias, reactor);
//				} else if (path.equals(ReactorFactory.H2_FRAME_PROP_PATH)) {
//					ReactorFactory.h2FrameHash.put(alias, reactor);
//				} else if (path.equals(ReactorFactory.NATIVE_FRAME_PROP_PATH)) {
//					ReactorFactory.nativeFrameHash.put(alias, reactor);
//				} else if (path.equals(ReactorFactory.R_FRAME_PROP_PATH)) {
//					ReactorFactory.rFrameHash.put(alias, reactor);
//				} else if (path.equals(ReactorFactory.TINKER_FRAME_PROP_PATH)) {
//					ReactorFactory.tinkerFrameHash.put(alias, reactor);
//				}
				return properties;
			}

		} catch (ClassNotFoundException e) {
			System.out.println("Reactor not found in the file: " + path);
		}
		return null;
	}

	// this method will use the updated properties object to rewrite the prop
	// file
	private void updatePropFile(Properties prop, String path) {
		try {
			PrintWriter pw = new PrintWriter(new File(path));
			StringBuilder sb = new StringBuilder();
			Object[] keys = prop.keySet().toArray();
			Arrays.sort(keys);
			for (Object operation: keys) {
				Object reactor = prop.get(operation.toString());
				sb.append(operation + " " + reactor + "\n");
			}
			pw.write(sb.toString());
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(keysToGet[0])) {
			return "The name of the reactor";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
