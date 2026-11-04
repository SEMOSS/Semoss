package prerna.reactor.collaboration;

import java.util.Map;

import prerna.collaboration.BrainDraftFormat;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainLearnDraftFormat();
// The signature and font the owner's recent sent mail uses; the caller saves them as draft settings
public class BrainLearnDraftFormatReactor extends AbstractCollaborationReactor {

	public BrainLearnDraftFormatReactor() {
		this.keysToGet = new String[0];
	}

	@Override
	public NounMetadata execute() {
		try {
			Map<String, Object> learned = BrainDraftFormat.learn(getUser());
			return mapResult(learned);
		} catch (SemossPixelException | IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new SemossPixelException("Your sent mail could not be read: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reads the signature and font the owner's recent sent mail uses, for their Collaboration draft settings";
	}
}
