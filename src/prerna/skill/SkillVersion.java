package prerna.skill;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

public class SkillVersion {

	private final String skillId;
	private final int version;
	private final String content;
	private final String changeNotes;
	private final Timestamp createdAt;

	public SkillVersion(String skillId, int version, String content, String changeNotes, Timestamp createdAt) {
		this.skillId = skillId;
		this.version = version;
		this.content = content;
		this.changeNotes = changeNotes;
		this.createdAt = createdAt;
	}

	public String getSkillId() {
		return skillId;
	}

	public int getVersion() {
		return version;
	}

	public String getContent() {
		return content;
	}

	public String getChangeNotes() {
		return changeNotes;
	}

	public Timestamp getCreatedAt() {
		return createdAt;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("skill_id", this.skillId);
		map.put("version", this.version);
		map.put("content", this.content);
		map.put("change_notes", this.changeNotes);
		map.put("created_at", this.createdAt);
		return map;
	}
}
