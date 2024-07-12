package prerna.reactor.utils;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityShareSessionUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GetShareSessionTokenReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetShareSessionTokenReactor.class);

	@Override
	public NounMetadata execute() {
		String token;
		try {
			token = SecurityShareSessionUtils.createShareToken(this.insight.getUser(), getSessionId(), getRouteId());
		} catch (SQLException e) {
			classLogger.error(e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not create share token.");
		}
		NounMetadata noun = new NounMetadata(token, PixelDataType.CONST_STRING);
		return noun;
	}

}
