package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class VizPkqlMetadata extends AbstractPkqlMetadata {

	// each of the subcomponenets that comprise a viz pkql
	
	VizComment comment;
	VizCommentEdit commentEdit;
	VizCommentRemoved commentRemoved;
	VizClose close;
	VizClone clone;
	VizLayout layout;
	VizLookAndFeel laf;
	VizConfigMap configMap;
	VizTool tool;
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> metadata = new Hashtable<String, Object>();
		// if panel changes config
		if (this.configMap != null) {
			metadata.put("configMap", this.configMap);
		}
		// if panel changes look and feel
		if (this.laf != null) {
			metadata.put("laf", this.laf);
		}
		// if new visualization is created
		if (this.layout != null) {
			metadata.put("layout", this.layout);
		}
		// if panel is cloned
		if (this.clone != null) {
			metadata.put("clone", this.clone);
		}
		// if panel is closed
		if (this.close != null) {
			metadata.put("close", this.close);
		}
		// handle comments
		if (this.comment != null) {
			metadata.put("comment", this.comment);
		}
		if(this.commentEdit != null) {
			metadata.put("commentEdited", this.comment);
		}
		if(this.commentRemoved != null) {
			metadata.put("commentRemoved", this.comment);
		}
		if(this.tool != null) {
			metadata.put("tool", this.tool);
		}
		
		return metadata;
	}

	@Override
	public String getExplanation() {
		String msg = "";

		// if panel changes config
		if (this.configMap != null) {
			msg += generateExplaination(this.configMap.getTemplate(), this.configMap.getTemplateData());
		}
		// if panel changes look and feel
		if (this.laf != null) {
			msg += generateExplaination(this.laf.getTemplate(), this.laf.getTemplateData());
		}
		// if new visualization is created
		if (this.layout != null) {
			msg += generateExplaination(this.layout.getTemplate(), this.layout.getTemplateData());
		}
		// if panel is cloned
		if (this.clone != null) {
			msg += generateExplaination(this.clone.getTemplate(), this.clone.getTemplateData());
		}
		// if panel is closed
		if (this.close != null) {
			msg += generateExplaination(this.close.getTemplate(), this.close.getTemplateData());
		}
		// handle comments
		if (this.comment != null) {
			msg += generateExplaination(this.comment.getTemplate(), this.comment.getTemplateData());
		}
		if(this.commentEdit != null) {
			msg += generateExplaination(this.commentEdit.getTemplate(), this.commentEdit.getTemplateData());
		}
		if(this.commentRemoved != null) {
			msg += generateExplaination(this.commentRemoved.getTemplate(), this.commentRemoved.getTemplateData());
		}
		if(this.tool != null) {
			msg += generateExplaination(this.tool.getTemplate(), this.tool.getTemplateData());
		}
			

		return msg;
	}

	public void addVizComment(String commentText) {
		this.comment = new VizComment(commentText);
	}
	
	public void addVizClose(String closeText) {
		this.close = new VizClose(closeText);
	}
	
	public void addVizClone(String oldPanelText, String newPanelText) {
		this.clone = new VizClone(oldPanelText, newPanelText);
	}
	
	public void addVizLayout(String visualType, List<String> columns) {
		this.layout = new VizLayout(visualType, columns);
	}
	
	public void AddVizLookAndFeel(Map laf) {
		this.laf = new VizLookAndFeel(laf);
	}
	
	public void addVizConfigMap(String width, String height, String top, String left) {
		this.configMap = new VizConfigMap(width, height, top, left);
	}

	public void editVizComment(String commentText) {
		this.commentEdit = new VizCommentEdit(commentText);
	}

	public void removeVizComment() {
		this.commentRemoved = new VizCommentRemoved();
	}

	public void addTools() {
		this.tool = new VizTool();
	}

}

/**
 * Interface defining the required values for each possible viz component 
 * that is defined within VizReactor.java
 * 
 * Need to be able to get the template format and the template data 
 * to feed into the generateExplaination method
 * 
 */
interface VizComponent {

	/**
	 * Get the string with the template for the explanation
	 * @return
	 */
	String getTemplate();
	
	/**
	 * Get a map with the keys matching the required fields
	 * in the template
	 * @return
	 */
	Map<String, Object> getTemplateData();
	
}

/**
 * Viz component for creating a new comment
 */
class VizComment implements VizComponent{
	
	String commentText;
	final String TEXT_TEMPLATE_FIELD_NAME = "text";

	public VizComment(String commentText) {
		this.commentText = commentText;
	}

	@Override
	public String getTemplate() {
		return "Added comment {{" + TEXT_TEMPLATE_FIELD_NAME + "}}";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(TEXT_TEMPLATE_FIELD_NAME, commentText);
		return templateData;
	}
}


/**
 * Viz component for editing a comment
 */
class VizCommentEdit implements VizComponent{
	
	String commentText;
	final String TEXT_TEMPLATE_FIELD_NAME = "commentText";

	public VizCommentEdit(String commentText) {
		this.commentText = commentText;
	}

	@Override
	public String getTemplate() {
		return "Edited comment {{" + TEXT_TEMPLATE_FIELD_NAME + "}}";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(TEXT_TEMPLATE_FIELD_NAME, commentText);
		return templateData;
	}
}

/**
 * Viz component for removing a comment
 */
class VizCommentRemoved implements VizComponent{

	public VizCommentRemoved() {
	}

	@Override
	public String getTemplate() {
		return "Removed comment";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		return templateData;
	}
}

/**
 * Viz component for changing tools
 */
class VizTool implements VizComponent{

	public VizTool() {
	}

	@Override
	public String getTemplate() {
		return "Changed tools";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		return templateData;
	}
}
/**
 * Viz component for a viz-close action
 */
class VizClose implements VizComponent {
	
	String closeText;
	final String TEXT_TEMPLATE_FIELD_NAME = "closedPanel";
	
	public VizClose(String closeText) {
		this.closeText = closeText;
	}
	
	@Override
	public String getTemplate() {
		return "Closed {{"+TEXT_TEMPLATE_FIELD_NAME+"}}";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(TEXT_TEMPLATE_FIELD_NAME, closeText);
		return templateData;
	}
}

/**
 * Viz component for creating a clone
 */
class VizClone implements VizComponent {
	
	String oldPanel;
	String newPanel;

	final String OLD_PANEL_TEXT_TEMPLATE_FIELD_NAME = "oldPanel";
	final String NEW_PANEL_TEXT_TEMPLATE_FIELD_NAME = "newPanel";

	public VizClone(String oldPanel, String newPanel) {
		this.oldPanel = oldPanel;
		this.newPanel = newPanel;
	}
	
	@Override
	public String getTemplate() {
		return "Cloned {{" + OLD_PANEL_TEXT_TEMPLATE_FIELD_NAME + "}} to panel[{{" +
				NEW_PANEL_TEXT_TEMPLATE_FIELD_NAME + "}}]";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(OLD_PANEL_TEXT_TEMPLATE_FIELD_NAME, oldPanel);
		templateData.put(NEW_PANEL_TEXT_TEMPLATE_FIELD_NAME, newPanel);
		return templateData;
	}
}

/**
 * Viz component for layout modifications
 */
class VizLayout implements VizComponent {

	String visualType;
	List<String> columns;
	
	final String LAYOUT_TEMPLATE_FIELD_NAME = "layout";
	final String COLUMNS_TEMPLATE_FIELD_NAME = "columns";
	
	public VizLayout(String visualType, List<String> columns) {
		this.visualType = visualType;
		this.columns = columns;
	}
	
	@Override
	public String getTemplate() {
		return "Created {{" + LAYOUT_TEMPLATE_FIELD_NAME + "}} visualization using {{"
				+ COLUMNS_TEMPLATE_FIELD_NAME + "}}";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(LAYOUT_TEMPLATE_FIELD_NAME, visualType);
		templateData.put(COLUMNS_TEMPLATE_FIELD_NAME, columns);
		return templateData;
	}
}

/**
 * Viz component for a look-and-feel actions
 */
class VizLookAndFeel implements VizComponent {
	
	Map laf;
	final String TEXT_TEMPLATE_FIELD_NAME = "laf";
	
	public VizLookAndFeel(Map laf) {
		this.laf = laf;
	}
	
	@Override
	public String getTemplate() {
		return "Changed look and feel {{" + TEXT_TEMPLATE_FIELD_NAME + "}}";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(TEXT_TEMPLATE_FIELD_NAME, laf);
		return templateData;
	}
}

/**
 * Viz component for a configuring size
 */
class VizConfigMap implements VizComponent {
	
	String width;
	String height;
	String top;
	String left;

	final String WIDTH_TEMPLATE_FIELD_NAME = "width";
	final String HEIGHT_TEMPLATE_FIELD_NAME = "height";
	final String TOP_TEMPLATE_FIELD_NAME = "top";
	final String LEFT_TEMPLATE_FIELD_NAME = "left";

	public VizConfigMap(String width, String height, String top, String left) {
		this.width = width;
		this.height = height;
		this.top = top;
		this.left = left;
	}
	
	@Override
	public String getTemplate() {
		return "Panel width: {{" + WIDTH_TEMPLATE_FIELD_NAME + "}}, height: {{"
				+ HEIGHT_TEMPLATE_FIELD_NAME + "}}, top: {{"
				+ TOP_TEMPLATE_FIELD_NAME + "}}, left: {{"
				+ LEFT_TEMPLATE_FIELD_NAME + "}}";
	}

	@Override
	public Map<String, Object> getTemplateData() {
		Map<String, Object> templateData = new Hashtable<String, Object>();
		templateData.put(WIDTH_TEMPLATE_FIELD_NAME, width);
		templateData.put(HEIGHT_TEMPLATE_FIELD_NAME, height);
		templateData.put(TOP_TEMPLATE_FIELD_NAME, top);
		templateData.put(LEFT_TEMPLATE_FIELD_NAME, left);
		return templateData;
	}
}
