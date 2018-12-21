package prerna.om;

public class RemoteItem extends AbstractValueObject {

	/**
	 * This class represents a remote file item
	 */
	
	String id = null;
	String name = null;
	String description = null;
	String folder = null;
	String type = null;
	String path = null;
	String url = null;
	
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
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}
	
	public String getFolder() {
		return folder;
	}
	
	public void setFolder(String folder) {
		this.folder = folder;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public String getPath() {
		return path;
	}
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
}
