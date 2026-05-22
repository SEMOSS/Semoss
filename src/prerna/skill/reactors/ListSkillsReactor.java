package prerna.skill.reactors;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.skill.SkillHelper;

public class ListSkillsReactor extends AbstractReactor {

	public ListSkillsReactor() {
		this.keysToGet = new String[] { "filterWord", "tags", "limit", "offset" };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filterWord = this.keyValue.get("filterWord");
		List<String> tags = getListString("tags", List.of());
		int limit = getInt("limit", 100);
		int offset = getInt("offset", 0);

		List<Map<String, Object>> results = SkillHelper.listSkills(filterWord, tags, limit, offset);
		return new NounMetadata(results, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
			return "Lists all active skills with optional filtering by keyword, tags, limit, and offset.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
			if ("filterWord".equals(key)) {
					return "Optional keyword to filter skills by name or description.";
			} else if ("tags".equals(key)) {
					return "Optional list of tags to filter skills by.";
			} else if ("limit".equals(key)) {
					return "Optional maximum number of skills to return (default 100).";
			} else if ("offset".equals(key)) {
					return "Optional number of skills to skip for pagination (default 0).";
			}
			return super.getDescriptionForKey(key);
	}
}
