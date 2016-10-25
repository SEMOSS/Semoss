package prerna.sablecc;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.VizPkqlMetadata;

public abstract class AbstractVizReactor extends AbstractReactor {

	public IPkqlMetadata getPkqlMetadata() {
		VizPkqlMetadata metadata = new VizPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.VIZ));
		if (myStore.containsKey("configMap")) {
			configMap(metadata);
		}
		// if panel changes look and feel
		if (myStore.containsKey("lookAndFeel")) {
			lookAndFeel(metadata);
		}
		// if new visualization is created
		if (myStore.containsKey("layout")) {
			layout(metadata);
		}
		// if panel is cloned
		if (myStore.containsKey("clone")) {
			clonePanel(metadata);
		}
		// if panel is closed
		if (myStore.containsKey("closedPanel")) {
			closePanel(metadata);
		}
		// handle comments
		if (myStore.containsKey("commentAdded")) {
			commentAdded(metadata);
		}
		if(myStore.containsKey("commentEdited")) {
			commentEdited(metadata);
		}
		if(myStore.containsKey("commentRemoved")) {
			commentRemoved(metadata);
		}
		
		return metadata;
	}
	
	/**
	 * Add to metadta a comment removed operation
	 * @param metadata
	 */
	protected void commentRemoved(VizPkqlMetadata metadata) {
		metadata.removeVizComment();			
	}

	/**
	 * Add to metadta a comment edit operation
	 * @param metadata
	 */
	protected void commentEdited(VizPkqlMetadata metadata) {
		Map commentMap = (HashMap) myStore.get("commentEdited");
		String commentText = (String) commentMap.get("text");
		metadata.editVizComment(commentText);		
	}

	/**
	 * Add to metadta a comment add operation
	 * @param metadata
	 */
	protected void commentAdded(VizPkqlMetadata metadata) {
		Map commentMap = (HashMap) myStore.get("commentAdded");
		String commentText = (String) commentMap.get("text");
		metadata.addVizComment(commentText);
	}

	/**
	 * Add to metadata a panel close operation
	 * @param metadata
	 * @return
	 */
	protected void closePanel(VizPkqlMetadata metadata) {
		String closedPanel = (String) myStore.get("closedPanel");
		closedPanel = closedPanel.substring(0, closedPanel.indexOf('.'));
		metadata.addVizClose(closedPanel);
	}

	/**
	 * Add to metadata a close panel operation
	 * @param metadata
	 * @return
	 */
	protected void clonePanel(VizPkqlMetadata metadata) {
		String oldPanel = (String) myStore.get("oldPanel");
		oldPanel = oldPanel.substring(0, oldPanel.indexOf('.'));
		String newPanel = (String) myStore.get("clone");
		metadata.addVizClone(oldPanel, newPanel);
	}

	/**
	 * Add to metadata a layout modification
	 * @param metadata
	 */
	protected void layout(VizPkqlMetadata metadata) {
		String visualType = (String) myStore.get("layout");
		Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.TERM);
		if(columns == null || columns.isEmpty()) {
			columns = new Vector<String>();
			columns.add("All Columns Selected");
		} 
		// TODO: really need to not do this...
		// super hack!
		else {
			for(int i = 0; i < columns.size(); i++) {
				String col = columns.get(i);
				if(col.startsWith("c:") && !col.contains("m:")) {
					col = col.replace("c:", "").trim();
				}
				columns.set(i, col);
			}
		}
		metadata.addVizLayout(visualType, columns);
	}

	/**
	 * Add to the metadata a look-and-feel operation
	 * @param metadata
	 */
	protected void lookAndFeel(VizPkqlMetadata metadata) {
		Map laf = (HashMap) myStore.get("lookAndFeel");
		metadata.AddVizLookAndFeel(laf);
	}

	/**
	 * Add to the metadata a config map operation
	 * @param metadata
	 */
	protected void configMap(VizPkqlMetadata metadata) {
		Map config = (HashMap) myStore.get("configMap");
		Map size = (Map) config.get("size");
		Object position = config.get("position");
		
		String width = (String) size.get("width");
		String height = (String) size.get("height");
		String top = "";
		String left = "";
		if(position instanceof Map) {
			Map positionMap = (Map) position;
			top = (String) positionMap.get("top");
			left = (String) positionMap.get("left");
		} else {
			top = left = "auto";
		}
		// add to the metadata here
		metadata.addVizConfigMap(width, height, top, left);
	}
	
}
