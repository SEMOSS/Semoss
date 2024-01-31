package prerna.reactor.utils;

import java.sql.SQLException;

import prerna.auth.utils.SecurityShareSessionUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetShareSessionTokenReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		String token;
		try {
			token = SecurityShareSessionUtils.createShareToken(this.insight.getUser(), getSessionId(), getRouteId());
		} catch (SQLException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		NounMetadata noun = new NounMetadata(token, PixelDataType.CONST_STRING);
		return noun;
	}

}
