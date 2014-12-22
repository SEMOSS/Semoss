/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.main.listener.specific.tap;

import java.awt.Desktop;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
		} catch(RuntimeException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
			} catch (RuntimeException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return JSValue.create(gson.toJson(allHash).replaceAll("'",""));
	}
	
}
