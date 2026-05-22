package prerna.skill.reactors;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class GetSkillVersionsReactor extends AbstractReactor {

	public GetSkillVersionsReactor() {
		this.keysToGet = new String[] { "skillId" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");
		List<Map<String, Object>> versions = SkillHelper.getSkillVersions(skillId);
		return new NounMetadata(versions, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
			return "Retrieves all version snapshots for a skill, ordered by version number descending.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("skillId".equals(key)) {
					return "Required ID of the skill to retrieve versions for.";
			}
			return super.getDescriptionForKey(key);
	}
}
