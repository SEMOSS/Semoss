package prerna.ui.helpers;

import prerna.ui.components.api.IPlaySheet;

public class PlaysheetRecreateRunner implements Runnable{

	IPlaySheet playSheet = null;
	
	public PlaysheetRecreateRunner(IPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}
	
	@Override
	public void run() {
		playSheet.redoView();
	}
	
	public void setPlaySheet(IPlaySheet playSheet)
	{
		this.playSheet = playSheet;
	}

}
