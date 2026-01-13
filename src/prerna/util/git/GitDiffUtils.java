/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.git;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.revwalk.RevTree;

public class GitDiffUtils {


	public static String printDiff(String gitFolder, String comm1, String comm2, String fileName) throws Exception
	{
		StringBuilder differ = new StringBuilder();

		try(Git thisGit = Git.open(new File(gitFolder))) {
		//Git thisGit = Git.open(new File(gitFolder));
				
		RevTree t1 = GitRepoUtils.findCommit(gitFolder, comm1).getTree();
		RevTree t2 = GitRepoUtils.findCommit(gitFolder, comm2).getTree();
		
		OutputStream outputStream = new ByteArrayOutputStream();
		DiffFormatter df = new DiffFormatter( outputStream); // use NullOutputStream.INSTANCE if you don't need the diff output
		df.setRepository( thisGit.getRepository() );
		
		List<DiffEntry> entries = df.scan( t1, t2);	
		//df.format(entries);
		

		/*CanonicalTreeParser ctp1 = null;
		try( ObjectReader reader = thisGit.getRepository().newObjectReader() ) {
		    ctp1 =  new CanonicalTreeParser( null, reader, comm1.getId() );
		  }

		CanonicalTreeParser ctp2 = null;
		try( ObjectReader reader = thisGit.getRepository().newObjectReader() ) {
		    ctp2 =  new CanonicalTreeParser( null, reader, comm2.getId() );
		  }
		*/
		
		//entries = thisGit.diff().setOldTree(ctp1).setNewTree(ctp2).call();
		
		//System.out.println(outputStream.toString());
		
		/*ObjectId treeId = thisGit.getRepository().resolve(comm1.getId());
		try( ObjectReader reader = repository.newObjectReader() ) {
		  treeParser.reset( reader, treeId );
		}		*/
		
		for(int entryIndex = 0;entryIndex < entries.size();entryIndex++)
		{
			DiffEntry thisEntry = entries.get(entryIndex);
			if(fileName != null && thisEntry.getNewPath().equalsIgnoreCase(fileName))
			{
				System.out.println(thisEntry.getChangeType());
				df.format(thisEntry);
				differ.append((outputStream.toString()));
				differ.append("\n");
				//System.out.println(thisEntry.get)
			}
		}
		
		/*
		String output = outputStream.toString();
		BufferedReader sr = new BufferedReader(new StringReader(output));
		String data = null;
		while((data = sr.readLine())!= null)
		{
			System.out.println(">>>" + data);
		}
		*/
		}
		
		return differ.toString();
	}

}
