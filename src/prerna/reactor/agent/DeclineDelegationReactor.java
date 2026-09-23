package prerna.reactor.agent;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.HumanDelegationService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Declines a delegation assigned to the logged-in user and closes it.
 *
 * <pre>{@code
 * DeclineDelegation(actionId=["<actionId>"], reason=["Not my area"]);
 * }</pre>
 */
public class DeclineDelegationReactor extends AbstractReactor {

	private static final String ACTION_ID_KEY = "actionId";
	private static final String REASON_KEY = "reason";

	public DeclineDelegationReactor() {
		this.keysToGet = new String[] { ACTION_ID_KEY, REASON_KEY };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String actionId = StringUtils.trimToNull(this.keyValue.get(ACTION_ID_KEY));
		if (actionId == null) {
			throw new IllegalArgumentException("actionId is required");
		}
		String reason = StringUtils.trimToNull(this.keyValue.get(REASON_KEY));
		return new NounMetadata(HumanDelegationService.decline(this.insight, actionId, reason), PixelDataType.MAP,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Decline a delegation assigned to the logged-in user. The optional reason is sent to the requester.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION_ID_KEY)) {
			return "The delegation's action ID, as listed by GetAssignedDelegations.";
		}
		if (key.equals(REASON_KEY)) {
			return "Optional reason sent back to the requester.";
		}
		return super.getDescriptionForKey(key);
	}
}
