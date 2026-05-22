package prerna.skill.reactors;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class VersionSkillReactor extends AbstractReactor {

	public VersionSkillReactor() {
		this.keysToGet = new String[] { "skillId", "changeNotes" };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String skillId = this.keyValue.get("skillId");
		String changeNotes = this.keyValue.get("changeNotes");

		Map<String, Object> versionInfo = SkillHelper.versionSkill(skillId, changeNotes);
		if (versionInfo == null) {
			return getError("Failed to create skill version");
		}
		return new NounMetadata(versionInfo, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
			return "Creates a new version snapshot of a skill's current content, incrementing its version number.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("skillId".equals(key)) {
					return "Required ID of the skill to version.";
			} else if ("changeNotes".equals(key)) {
					return "Optional notes describing what changed in this version.";
			}
			return super.getDescriptionForKey(key);
	}
}
