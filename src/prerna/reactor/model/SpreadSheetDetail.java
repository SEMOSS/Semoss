package prerna.reactor.model;

public class SpreadSheetDetail {

	private String id;
	private String name;
	private String createdAt;
	private String updatedAt;
	private String spreadSheetId;
	private String userId;
	private String userName;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}
	public String getUpdatedAt() {
		return updatedAt;
	}
	public void setUpdatedAt(String updatedAt) {
		this.updatedAt = updatedAt;
	}
	public String getSpreadSheetId() {
		return spreadSheetId;
	}
	public void setSpreadSheetId(String spreadSheetId) {
		this.spreadSheetId = spreadSheetId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	@Override
	public String toString() {
		return "SpreadSheetDetail [id=" + id + ", name=" + name + ", createdAt=" + createdAt + ", updatedAt="
				+ updatedAt + ", spreadSheetId=" + spreadSheetId + ", userId=" + userId + ", userName=" + userName
				+ "]";
	}
}