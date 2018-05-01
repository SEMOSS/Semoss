package prerna.om;

public class EntityResolution {

	private String entity_name;
	private String entity_type;
	private String wiki_url;
	private String content;
	private String content_subtype;

	public EntityResolution() {
		
	}

	/*
	 * This is just a struct
	 * Define setters and getters for the class variables
	 */
	
	
	public String getEntity_name() {
		return entity_name;
	}

	public void setEntity_name(String entity_name) {
		this.entity_name = entity_name;
	}

	public String getEntity_type() {
		return entity_type;
	}

	public void setEntity_type(String entity_type) {
		this.entity_type = entity_type;
	}

	public String getWiki_url() {
		return wiki_url;
	}

	public void setWiki_url(String wiki_url) {
		this.wiki_url = wiki_url;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getContent_subtype() {
		return content_subtype;
	}

	public void setContent_subtype(String content_subtype) {
		this.content_subtype = content_subtype;
	}
	
	
}
