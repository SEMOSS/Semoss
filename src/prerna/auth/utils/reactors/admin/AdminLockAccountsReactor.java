package prerna.auth.utils.reactors.admin;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminLockAccountsReactor extends AbstractReactor {

	public AdminLockAccountsReactor() {
		this.keysToGet = new String[] {"days"};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		int numDays = 90;
		String input = this.keyValue.get(this.keysToGet[0]);
		if(input != null && !(input = input.trim()).isEmpty()) {
			numDays = (int) Double.parseDouble(input);
		}
		
		int numLocked = adminUtils.lockAccounts(numDays);
		NounMetadata noun = new NounMetadata(numLocked, PixelDataType.CONST_INT);
		noun.addAdditionalReturn(getSuccess("Number of accounts locked = " + numLocked));
		return noun;
	}
	
}
