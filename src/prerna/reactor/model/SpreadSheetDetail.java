package prerna.reactor.model;

public class SpreadSheetDetail {

	private String createdAt;
	private String name;
	private String userId;
	public String getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	@Override
	public String toString() {
		return "SpreadSheetDetail [createdAt=" + createdAt + ", name=" + name + ", userId=" + userId + "]";
	}
}