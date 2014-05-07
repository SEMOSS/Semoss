package prerna.ui.main.listener.impl;

import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import com.teamdev.jxbrowser.chromium.Browser;

public class BrowserZoomKeyListener extends KeyAdapter{
	Browser browser = null;
	public BrowserZoomKeyListener (Browser browser)
	{
		this.browser = browser;
	}
	
	@Override
    public void keyPressed(KeyEvent e) {
    	double zoomLevel = browser.getZoomLevel();

    	if ((e.getKeyCode() == 61) && ((e.getModifiers() & KeyEvent.CTRL_MASK) != 0) && zoomLevel<4.0) {
            zoomLevel = zoomLevel+0.5;
        }
    	if ((e.getKeyCode() == 45) && ((e.getModifiers() & KeyEvent.CTRL_MASK) != 0) && zoomLevel>-4.0) {
            zoomLevel = zoomLevel-0.5;
        }
    	browser.setZoomLevel(zoomLevel);
    }

}
