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
package prerna.ui.components;

import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to manage the right view in SEMOSS.
 */
public class RightViewLayoutManager implements LayoutManager
{
	public RightViewLayoutManager()
	{
		
	}
	 private List<Component> special = new ArrayList<Component> ();

	    public void addLayoutComponent ( String name, Component comp )
	    {
	        if ( name != null )
	        {
	            special.add ( comp );
	        }
	    }

	    public void removeLayoutComponent ( Component comp )
	    {
	        special.remove ( comp );
	    }

	    public Dimension preferredLayoutSize ( Container parent )
	    {
	        Dimension ps = new Dimension ();
//	        for ( Component component : parent.getComponents () )
//	        {
//	            if ( !special.contains ( component ) )
//	            {
//	                Dimension cps = component.getPreferredSize ();
//	                ps.width = Math.max ( ps.width, cps.width );
//	                ps.height = Math.max ( ps.height, cps.height );
//	            }
//	        }
	        return ps;
	    }

	    public Dimension minimumLayoutSize ( Container parent )
	    {
	        return preferredLayoutSize ( parent );
	    }

	    public void layoutContainer ( Container parent )
	    {
	        Insets insets = parent.getInsets ();
	        for ( Component component : parent.getComponents () )
	        {
	            if ( !special.contains ( component ) )
	            {
	                component.setBounds ( insets.left, insets.top,
	                        parent.getWidth () - insets.left - insets.right,
	                        parent.getHeight () - insets.top - insets.bottom );
	            }
	            else
	            {
	                Dimension ps = component.getPreferredSize ();
	                component.setBounds ( parent.getWidth () - insets.right - 2 - ps.width,
	                        insets.top + 2, ps.width, ps.height );
	            }
	        }
	    }
}
