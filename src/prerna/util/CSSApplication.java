package prerna.util;

import aurelienribon.ui.css.Style;
import aurelienribon.ui.css.StyleException;

public class CSSApplication {
	public CSSApplication(Object object, String cssLine)
	{
	try {
		Style.unregisterTargetClassName(object);
		Style.registerTargetClassName(object, cssLine);
		Style.apply(object, new Style(getClass().getResource("styles.css")));
	} catch (StyleException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
		}
	}
	
	public CSSApplication(Object object)
	{
	try {
		Style.apply(object, new Style(getClass().getResource("styles.css")));
	} catch (StyleException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
		}
	}
}
