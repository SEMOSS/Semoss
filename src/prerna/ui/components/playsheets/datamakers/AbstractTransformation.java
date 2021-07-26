package prerna.ui.components.playsheets.datamakers;

import java.util.Map;

@Deprecated
public abstract class AbstractTransformation implements ISEMOSSTransformation{

	protected String id;
	protected Map<String, Object> props;
	
	@Override
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public String getId() {
		return this.id;
	}
	
	@Override
	public void setProperties(Map<String, Object> props) {
		this.props = props;
	}	
}
