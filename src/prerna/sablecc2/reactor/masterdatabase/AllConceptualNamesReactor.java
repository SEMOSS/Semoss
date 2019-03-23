package prerna.sablecc2.reactor.masterdatabase;

import java.util.Collection;
import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AllConceptualNamesReactor extends AbstractReactor {

	/**
	 * Return all the conceptual names
	 */
	
	@Override
	public NounMetadata execute() {
		// need to take into consideration security
		Collection<String> conceptualNames = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			List<String> engineFilters = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			conceptualNames = MasterDatabaseUtility.getAllConceptualNames(engineFilters);
		} else {
			conceptualNames = MasterDatabaseUtility.getAllConceptualNames();
		}
		
		return new NounMetadata(conceptualNames, PixelDataType.CONST_STRING);
	}
	
}
