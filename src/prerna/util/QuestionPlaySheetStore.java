package prerna.util;

import java.beans.PropertyVetoException;
import java.util.Hashtable;

import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JToggleButton;

import prerna.ui.components.api.IPlaySheet;

public class QuestionPlaySheetStore extends Hashtable<String, IPlaySheet> {
	
	public static QuestionPlaySheetStore store = null;
	//public Vector 
	public static int count = 0;
	public static IPlaySheet activeSheet = null;
	
	protected QuestionPlaySheetStore()
	{
		// do nothing
	}
	
	public boolean hasMorePlaySheet()
	{
		return false;
	}
	
	public static QuestionPlaySheetStore getInstance()
	{
		if(store == null)
		{
			store = new QuestionPlaySheetStore();
		}
		return store; 
	}

	
	public IPlaySheet put(String question, IPlaySheet sheet)
	{
		IPlaySheet ret = super.put(question, sheet);
		return ret;
	}
	
	public void setActiveSheet(IPlaySheet sheet)
	{
		// need to clear when the internal frame is closed
		activeSheet = sheet;
	}
	
	public IPlaySheet getActiveSheet()
	{
		// need to clear when the internal frame is closed
		return activeSheet;
	}
	
	public IPlaySheet get(String question)
	{
		return super.get(question);
	}
	
	public void remove(String question)
	{
		super.remove(question);
		//System.err.println("Into the part for selecting the active sheet");
		if(activeSheet != null && activeSheet.getQuestionID().equalsIgnoreCase(question))
		{
			activeSheet = null;
		}
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		//System.err.println("Into the part for selecting the active sheet    ");
		
		 JInternalFrame [] frames = pane.getAllFrames();
		 boolean foundFrame = false;
		 for(int frameIndex = 0;frameIndex < frames.length;frameIndex++)
		 {
			 try {
				if(frames[frameIndex].isVisible() && frames[frameIndex] instanceof IPlaySheet)
				 {
					 //System.out.println("Frame is visible");
					 activeSheet = (IPlaySheet)frames[frameIndex];
					 frames[frameIndex].setSelected(false);
					 frames[frameIndex].setSelected(true);
					 foundFrame = true;
					 break;
				 }
			} catch (PropertyVetoException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 if(!foundFrame)
		 {
			JToggleButton append = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.APPEND);
			append.setEnabled(false);
			append.setSelected(false);

			JToggleButton extend = (JToggleButton)DIHelper.getInstance().getLocalProp(Constants.EXTEND);
			extend.setEnabled(false);
			extend.setSelected(false);
		 }
	}
	
	public int getCount()
	{
		count++;
		return count;
	}
}
