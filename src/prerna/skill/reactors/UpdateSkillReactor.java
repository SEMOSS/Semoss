package prerna.skill.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class UpdateSkillReactor extends AbstractReactor {

	public UpdateSkillReactor() {
		this.keysToGet = new String[] { "skillId", "content", "description" };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");
		String content = this.keyValue.get("content");
		String description = this.keyValue.get("description");

		boolean updated = SkillHelper.updateSkill(skillId, content, description);
		return new NounMetadata(updated, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
			return "Updates the content and/or description of an existing skill by its ID.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("skillId".equals(key)) {
					return "Required ID of the skill to update.";
			} else if ("content".equals(key)) {
					return "Required updated content body of the skill.";
			} else if ("description".equals(key)) {
					return "Optional updated description of the skill.";
			}
			return super.getDescriptionForKey(key);
	}
}
