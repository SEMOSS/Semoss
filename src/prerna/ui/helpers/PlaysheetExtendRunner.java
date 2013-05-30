package prerna.ui.helpers;

import prerna.ui.components.api.IPlaySheet;

public class PlaysheetExtendRunner implements Runnable{

	IPlaySheet playSheet = null;
	
	public PlaysheetExtendRunner(IPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}

	@Override
	public void run() {
		playSheet.extendView();
	}
	
	public void setPlaySheet(IPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}

}
