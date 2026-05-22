package prerna.skill.reactors;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class GetSkillReactor extends AbstractReactor {

	public GetSkillReactor() {
		this.keysToGet = new String[] { "skillId" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");

		Map<String, Object> result = SkillHelper.getSkill(skillId);
		if (result == null) {
			return getError("Skill not found");
		}
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
			return "Retrieves a single skill by its ID.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("skillId".equals(key)) {
					return "Required ID of the skill to retrieve.";
			}
			return super.getDescriptionForKey(key);
	}
}