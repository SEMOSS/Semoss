package prerna.io.connector.google.meet;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleMeetListMeetingsReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GoogleMeetListMeetingsReactor.class);

    public GoogleMeetListMeetingsReactor() {
		
	}
    
    @Override
    public NounMetadata execute() {
        try {
            User user = this.insight.getUser();

            String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);

            Map<String, Object> result = GoogleMeetHelper.listMeetings(accessToken);
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);

        } catch (SemossPixelException e) {
            classLogger.error("Error while listing Google Meet meetings", e);
            throw e;

        } catch (Exception e) {
            classLogger.error("Failed to list Google Meet meetings", e);
            throw new SemossPixelException(
                    "An error occurred listing meetings. Error message: " + e.getMessage()
            );
        }
    }

    @Override
    public String getReactorDescription() {
        return "List Google Meet meetings. This reactor is called after Google Login.";
    }
}