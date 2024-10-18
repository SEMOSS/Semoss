package prerna.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ContactUsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ContactUsReactor.class);

	public ContactUsReactor() {

		this.keysToGet = new String[] { "email", "message" };

	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String email = keyValue.get(keysToGet[0]).toString();
		String message = keyValue.get(keysToGet[1]).toString();
		

		IDatabaseEngine dbengine = Utility.getDatabase("3035d61f-bdee-44c2-802c-06278963ee1a");
		boolean flag = false;

		try {

			String query = "INSERT INTO public.contactus (emailid, message) VALUES (" + "'" + email + "'" + "," + "'"
					+ message + "'" + ")";

			dbengine.insertData(query);
			flag = true;

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Exception during inserting data in contact us table");
		}
		if (flag == true) {
			return new NounMetadata("Data inserted successfully into contact us table", PixelDataType.CONST_STRING,
					PixelOperationType.OPERATION);

		}
		return null;
	}

}
