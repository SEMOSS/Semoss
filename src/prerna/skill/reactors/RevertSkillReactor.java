package prerna.skill.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class RevertSkillReactor extends AbstractReactor {

	public RevertSkillReactor() {
		this.keysToGet = new String[] { "skillId", "version" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");
		int version = getInt("version", -1);
		boolean reverted = SkillHelper.revertSkill(skillId, version);
		return new NounMetadata(reverted, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
			return "Reverts a skill's content to a previously saved version by skill ID and version number.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("skillId".equals(key)) {
					return "Required ID of the skill to revert.";
			} else if ("version".equals(key)) {
					return "Required version number to revert the skill content to.";
			}
			return super.getDescriptionForKey(key);
	}
}
