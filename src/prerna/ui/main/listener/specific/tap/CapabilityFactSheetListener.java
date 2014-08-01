/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.specific.tap;

import java.awt.Desktop;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import prerna.ui.components.specific.tap.CapabilityFactSheet;
import prerna.ui.main.listener.impl.AbstractBrowserSPARQLFunction;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 */
public class CapabilityFactSheetListener extends AbstractBrowserSPARQLFunction {
	
	CapabilityFactSheet cfs;

	/**
	 * Method setCapabilityFactSheet
	 * @param cfs CapabilityFactSheet that this listener acts on.
	 */
	public void setCapabilityFactSheet(CapabilityFactSheet cfs)
	{
		this.cfs = cfs;
	}
	
	/**
	 * Method invoke.
	 * @param arg0 Object[]
	 * @return Object 
	 */
	@Override
	public JSValue invoke(JSValue... arg0) {		
		String capability = arg0[0].getString();
//		System.out.println("Capability chosen is "+capability);
		capability = (String)cfs.capabilityProcessed.get(capability);
		Hashtable allHash = cfs.processNewCapability(capability);
		Gson gson = new Gson();
		BufferedWriter out = null;
		try {
			File file = new File(DIHelper.getInstance().getProperty("BaseFolder") + "/html/MHS-FactSheets/export.json");
			out = new BufferedWriter(new FileWriter(file, true));
			out.append(gson.toJson(allHash));
			out.close();
		} catch(Exception e) {
			e.printStackTrace();
		}finally{
			try{
				if(out!=null)
					out.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
		if(Desktop.isDesktopSupported())
		{
			try {
				Desktop.getDesktop().browse(new URI((DIHelper.getInstance().getProperty("BaseFolder") + "/html/MHS-FactSheets/index.html").replace("\\", "/")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return JSValue.create(gson.toJson(allHash).replaceAll("'",""));
	}
	
}
