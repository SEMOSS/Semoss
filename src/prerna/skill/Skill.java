package prerna.skill;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Skill implements ISkill {

	private String skillId;
	private String skillName;
	private String description;
	private String content;
	private int version;
	private List<String> tags;
	private String ownerId;
	private String projectId;
	private Timestamp createdAt;
	private Timestamp updatedAt;
	private boolean active;

	public Skill(String skillId, String skillName, String description, String content, int version, List<String> tags,
			String ownerId, String projectId, Timestamp createdAt, Timestamp updatedAt, boolean active) {
		this.skillId = skillId;
		this.skillName = skillName;
		this.description = description;
		this.content = content;
		this.version = version;
		this.tags = tags == null ? new ArrayList<>() : new ArrayList<>(tags);
		this.ownerId = ownerId;
		this.projectId = projectId;
		this.createdAt = createdAt;
		this.updatedAt = updatedAt;
		this.active = active;
	}

	@Override
	public String getSkillId() {
		return this.skillId;
	}

	@Override
	public String getSkillName() {
		return this.skillName;
	}

	@Override
	public String getDescription() {
		return this.description;
	}

	@Override
	public String getContent() {
		return this.content;
	}

	@Override
	public int getVersion() {
		return this.version;
	}

	@Override
	public List<String> getTags() {
		return new ArrayList<>(this.tags);
	}

	@Override
	public String getOwnerId() {
		return this.ownerId;
	}

	@Override
	public String getProjectId() {
		return this.projectId;
	}

	@Override
	public Timestamp getCreatedAt() {
		return this.createdAt;
	}

	@Override
	public Timestamp getUpdatedAt() {
		return this.updatedAt;
	}

	@Override
	public boolean isActive() {
		return this.active;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("skill_id", this.skillId);
		map.put("skill_name", this.skillName);
		map.put("description", this.description);
		map.put("content", this.content);
		map.put("version", this.version);
		map.put("tags", new ArrayList<>(this.tags));
		map.put("owner_id", this.ownerId);
		map.put("project_id", this.projectId);
		map.put("created_at", this.createdAt);
		map.put("updated_at", this.updatedAt);
		map.put("is_active", this.active);
		return map;
	}
}
