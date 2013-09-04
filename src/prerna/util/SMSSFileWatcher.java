package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JList;

public class SMSSFileWatcher extends AbstractFileWatcher {

	@Override
	public void process(String fileName) {
		// TODO Auto-generated method stub
		try {
			//loadExistingDB();
			loadNewDB(fileName);							
		}catch(Exception ex)
		{ex.printStackTrace();}
	}
	
	public void loadExistingDB() throws Exception
	{
		File dir = new File(folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + "/" + fileNames[fileIdx];
				loadNewDB(fileNames[fileIdx]);
				//Utility.loadEngine(fileName, prop);				
			}catch(Exception ex)
			{
				ex.printStackTrace();
				logger.fatal("Engine Failed " + "./db/" + fileNames[fileIdx]);
			}
		}	

	}
	
	public void loadNewDB(String newFile) throws Exception
	{
		Properties prop = new Properties();
		prop.load(new FileInputStream(folderToWatch + "/"  +  newFile));

		Utility.loadEngine("./db/" +  newFile, prop);
		String engineName = prop.getProperty(Constants.ENGINE);
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		DefaultListModel listModel = (DefaultListModel) list.getModel();
		listModel.addElement(engineName);
		//list.setModel(listModel);
		list.setSelectedIndex(0);
		list.repaint();
				
		JComboBox exportDataDBComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SOURCE_COMBOBOX);
		DefaultComboBoxModel exportDataDBComboBoxModel = (DefaultComboBoxModel) exportDataDBComboBox.getModel();
		exportDataDBComboBoxModel.addElement(engineName);
		exportDataDBComboBox.repaint();
		
		JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
				Constants.MAIN_FRAME);
		frame2.repaint();
	}
	
	
	@Override
	public void loadFirst()
	{
		File dir = new File(folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + fileNames[fileIdx];
				Properties prop = new Properties();
				process(fileNames[fileIdx]);
			}catch(Exception ex)
			{
				logger.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
	}

	
	@Override
	public void run()
	{
		System.out.println("Starting thread");
		synchronized(monitor)
		{
			super.run();
		}
	}

}
