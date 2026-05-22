package prerna.skill.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class DeleteSkillReactor extends AbstractReactor {

	public DeleteSkillReactor() {
		this.keysToGet = new String[] { "skillId" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");
		boolean deleted = SkillHelper.deleteSkill(skillId);
		return new NounMetadata(deleted, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
			return "Permanently deletes a skill and all its versions by skill ID.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("skillId".equals(key)) {
					return "Required ID of the skill to delete.";
			}
			return super.getDescriptionForKey(key);
	}
}
