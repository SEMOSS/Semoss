package prerna.skill.reactors;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class CreateSkillReactor extends AbstractReactor {

	public CreateSkillReactor() {
		this.keysToGet = new String[] { "name", "content", "description", "tags", ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String name = this.keyValue.get("name");
		String content = this.keyValue.get("content");
		String description = this.keyValue.get("description");
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		List<String> tags = getListString("tags", List.of());

		Map<String, Object> result = SkillHelper.createSkill(user, name, content, description, tags, projectId);
		if (result == null) {
			return getError("Failed to create skill");
		}
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
			return "Creates a new skill with a name, content, and optional description, tags, and project association.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("name".equals(key)) {
					return "Required name for the skill.";
			} else if ("content".equals(key)) {
					return "Required content body of the skill.";
			} else if ("description".equals(key)) {
					return "Optional description of what the skill does.";
			} else if ("tags".equals(key)) {
					return "Optional list of tags to categorize the skill.";
			} else if ("project".equals(key)) {
					return "Optional project ID to associate the skill with.";
			}
			return super.getDescriptionForKey(key);
	}
}