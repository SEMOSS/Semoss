package prerna.skill.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class DeleteSkillVersionReactor extends AbstractReactor {

	public DeleteSkillVersionReactor() {
		this.keysToGet = new String[] { "skillId", "version" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");
		int version = getInt("version", -1);
		if (version < 1) {
			return getError("A valid version number is required.");
		}
		boolean deleted = SkillHelper.deleteSkillVersion(skillId, version);
		return new NounMetadata(deleted, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "Deletes a specific version snapshot of a skill. Cannot delete the last remaining version.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if ("skillId".equals(key)) {
			return "Required ID of the skill whose version is to be deleted.";
		} else if ("version".equals(key)) {
			return "Required version number to delete.";
		}
		return super.getDescriptionForKey(key);
	}
}
