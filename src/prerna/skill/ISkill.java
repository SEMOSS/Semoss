package prerna.skill;

import java.sql.Timestamp;
import java.util.List;

public interface ISkill {

	String getSkillId();

	String getSkillName();

	String getDescription();

	String getContent();

	int getVersion();

	List<String> getTags();

	String getOwnerId();

	String getProjectId();

	Timestamp getCreatedAt();

	Timestamp getUpdatedAt();

	boolean isActive();
}
