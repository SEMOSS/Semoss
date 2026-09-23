package prerna.reactor.agent;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.HumanDelegationService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the delegations assigned to the logged-in user, newest first.
 *
 * <pre>{@code
 * GetAssignedDelegations();
 * GetAssignedDelegations(status=["PENDING"]);
 * }</pre>
 */
public class GetAssignedDelegationsReactor extends AbstractReactor {

	private static final String STATUS_KEY = "status";

	public GetAssignedDelegationsReactor() {
		this.keysToGet = new String[] { STATUS_KEY };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String status = StringUtils.trimToNull(this.keyValue.get(STATUS_KEY));
		return new NounMetadata(HumanDelegationService.listAssigned(this.insight, status), PixelDataType.VECTOR,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "List delegations assigned to the logged-in user, with each delegation's room, question, context, and status.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(STATUS_KEY)) {
			return "Optional status filter: PENDING, RESPONDED, DECLINED, or CANCELLED.";
		}
		return super.getDescriptionForKey(key);
	}
}
