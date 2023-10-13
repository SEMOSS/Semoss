package prerna.reactor.masterdatabase;

import java.util.Collection;
import java.util.List;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AllConceptualNamesReactor extends AbstractReactor {

	/**
	 * Return all the conceptual names
	 */

	@Override
	public NounMetadata execute() {
		// need to take into consideration security
		List<String> engineFilters = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		Collection<String> conceptualNames = MasterDatabaseUtility.getAllConceptualNames(engineFilters);
		return new NounMetadata(conceptualNames, PixelDataType.CONST_STRING);
	}

}
