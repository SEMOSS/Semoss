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
package prerna.util;

import java.io.File;
import java.io.FilenameFilter;

/**
 * This class is used to filter property names.
 */
public class PropFilter implements FilenameFilter
{
	String ext = ".smss";
	/**
	 * Tests if a specified property should be included in the property file.
	 * @param dir File		Directory in which file was found.
	 * @param name String	Name of the property.
	
	 * @return boolean 		True if the name should be included in the property file. */
	@Override
	public boolean accept(File dir, String name) {
		return name.endsWith(ext);
	}
	
}
