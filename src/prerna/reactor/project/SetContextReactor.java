package prerna.reactor.project;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Deprecated
public class SetContextReactor extends AbstractReactor {

	// takes in a the name and engine and mounts the engine assets as that variable
	// name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then
	// later, they would like to use a different mapping

	public SetContextReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to set app context",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		organizeKeys();
		String context = keyValue.get(keysToGet[0]);
		if (context == null || (context = context.trim()).isEmpty()) {
			return getError("Must pass in a valid project id for the context value");
		}

		// need to replace the app with the
		boolean success = this.insight.setContext(context);
		// attempt once to directly map it with same name
		if (!success) {
			return getError("User does not have access to set the context to " + context);
		}

		return new NounMetadata("Successfully set context to '" + context, PixelDataType.CONST_STRING,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is deprecated. Please update to LoadApp(project='') instead";
	}

}
