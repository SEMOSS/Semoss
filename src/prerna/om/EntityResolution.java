/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
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
	 * This is just a struct Define setters and getters for the class variables
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
