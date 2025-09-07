package prerna.reactor.security;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class AdminGetRDFMapReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger();

	public AdminGetRDFMapReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException(
					"User is not an admin and does not have access. Please login as an admin");
		}

		StringBuilder currentRDFMapPropContent = null;
		try {
			currentRDFMapPropContent = DIHelper.getInstance().getConcealedRDFMapContents();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(
					String.format("Error occurred reading the RDF_Map.prop file. Detailed error = %s", e.getMessage()),
					e);
		}
		return new NounMetadata(currentRDFMapPropContent.toString(), PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Admin reactor to get the contents of the RDF_Map.prop file with secrets concealed";
	}

}
