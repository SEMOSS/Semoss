
package prerna.test;

import java.awt.FileDialog;
import java.awt.Frame;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RList;
import org.rosuda.JRI.RMainLoopCallbacks;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;

class TextConsole implements RMainLoopCallbacks
{
    public void rWriteConsole(Rengine re, String text, int oType) {
        System.out.print(text);
    }
    
    public void rBusy(Rengine re, int which) {
        System.out.println("rBusy("+which+")");
    }
    
    
    public String rReadConsole(Rengine re, String prompt, int addToHistory) {
        System.out.print(prompt);
        try {
            BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
            String s=br.readLine();
            return (s==null||s.length()==0)?s:s+"\n";
        } catch (Exception e) {
            System.out.println("jriReadConsole exception: "+e.getMessage());
        }
        return null;
    }
    
    public void rShowMessage(Rengine re, String message) {
        System.out.println("rShowMessage \""+message+"\"");
    }
	
    public String rChooseFile(Rengine re, int newFile) {
	FileDialog fd = new FileDialog(new Frame(), (newFile==0)?"Select a file":"Select a new file", (newFile==0)?FileDialog.LOAD:FileDialog.SAVE);
	fd.show();
	String res=null;
	if (fd.getDirectory()!=null) res=fd.getDirectory();
	if (fd.getFile()!=null) res=(res==null)?fd.getFile():(res+fd.getFile());
	return res;
    }
    
    public void   rFlushConsole (Rengine re) {
    }
	
    public void   rLoadHistory  (Rengine re, String filename) {
    }			
    
    public void   rSaveHistory  (Rengine re, String filename) {
    }			
}

public class TestR {
	
	DockerClient docker = null;
	String containerId = null;
	
	
    public void connectRServe(String host, int port)
    {
    	try {
			@SuppressWarnings("unused")
			RConnection rcon = new RConnection(host, port);
			org.rosuda.REngine.REXP rexp = rcon.eval("installed.packages();");
			System.out.println("Ok that worked.. " + rexp.toString());
			rexp = rcon.eval("a <- 'moron'; a");
			System.out.println("Ok that worked.. " + rexp.toString());
			// trying the method that fails
			rexp = rcon.eval("if(!exists(\"" + "default" + "\")) {" + "default" + "<- new.env();}");
			System.out.println("Ok that worked.. " + rexp.toString());

		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void stopContainer()
    {
    	try {
			docker.killContainer(containerId);
			docker.removeContainer(containerId);
		} catch (DockerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void listContainers()
    {
		try {
			List <Container> lc = docker.listContainers();
			for(int cIndex = 0;cIndex < lc.size();cIndex++)
			{
				Container c = lc.get(cIndex);
				System.out.println(c.id() + c.imageId());
				System.out.println(c.names().get(0));
			}
		} catch (DockerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    public void startContainer(String image_name)
    {
    	//docker.pull(image_name);
    	
    	// docker run -itd -p 6311:6311  deb-rserve1d /bin/bash -c "R CMD Rserve --RS-port 6311 && while :; do sleep 1;done"
    	
    	try {
    		
    		// configure port
    		
    		final String[] ports = {"6311"};
    		final Map<String, List<PortBinding>> portBindings = new HashMap<>();

    		for (String port : ports) {
    		    List<PortBinding> hostPorts = new ArrayList<>();
    		    hostPorts.add(PortBinding.of("HostPort", port));
    		    portBindings.put(port+"/tcp", hostPorts);
    		}


    		String [] cmd = {"sh", "-c", "rserve.sh"};//, "&&", "while :;", "do sleep 1;", "done"};
    		
    		HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
    		
    		
			ContainerConfig cInstance = ContainerConfig.builder()
										.hostConfig(hostConfig)
										.tty(true)
										.cmd(cmd)
										.image(image_name).build();
			
			ContainerCreation creation = docker.createContainer(cInstance);
			System.out.println("Created with id.... " + creation.id());
			
			containerId = creation.id();
			
			docker.startContainer(creation.id());
			
			
			
		} catch (DockerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }
    
    
    public void runDocker()
    {
    	try {
			//final DockerClient docker = DefaultDockerClient.fromEnv().build();
    		//final DockerClient docker = DefaultDockerClient.builder().uri(URI.create("tcp://192.168.99.100:2376")).build();
			
    		
    		DockerCertificates defaultCertificates = new DockerCertificates(Paths.get("C:/Users/pkapaleeswaran/.docker/machine/machines/default"));    
    		docker = DefaultDockerClient.builder()
    		                .uri("https://192.168.99.100:2376")
    		                .dockerCertificates(defaultCertificates)
    		                .build();
    		
    		
    		// list the containers
    		System.out.println("Docker host... " + docker.getHost());
			
			
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

    public static void main(String[] args) {
    	
    	TestR rc = new TestR();
    	//rc.runDocker();
    	//rc.startContainer("deb-rserve1e");
    	//rc.listContainers();
    	rc.connectRServe("192.168.99.100", 6311);
    	
    	//rc.stopContainer();
    	
	// just making sure we have the right version of everything
    	String tableTest = "helloworld";
    	String [] tables = tableTest.split("[.]");
    	String tableName = tables[tables.length-1];
    	
    	System.out.println("Table...  " + tableName);
    	
    	System.out.println("Printing the current library path" + System.getProperty("java.library.path"));
    	//System.setProperty("java.library.path", "C:/Program Files/R/R-3.1.2/bin/x64;C:/Users/pkapaleeswaran/Documents/R/win-library/3.1/rJava/jri");
    	System.out.println("Printing the current library path" + System.getProperty("java.library.path"));
    	//System.out.println(System.getProperty("java.path"));
    	//System.exit(1);
	if (!Rengine.versionCheck()) {
	    System.err.println("** Version mismatch - Java files don't match library version.");
	    System.exit(1);
	}
        System.out.println("Creating Rengine (with arguments)");
		// 1) we pass the arguments from the command line
		// 2) we won't use the main loop at first, we'll start it later
		//    (that's the "false" as second argument)
		// 3) the callbacks are implemented by the TextConsole class above
		Rengine re=new Rengine(args, true, null); // false, new TextConsole());
		/*, repos='http://cran.us.r-project.org' */
		System.out.println("version... " + re.eval("R.version['minor']").toString());
		System.out.println("installed... " + re.eval("'moron' %in% rownames(installed.packages()) == TRUE").asBool().isTRUE());
		System.out.println("DPLYR... " + re.eval("library(lattice)"));
	
		System.out.println("library(RJDBC)" + re.eval("library(RJDBC)"));
		System.out.println("library data table" + re.eval("library(data.table)" ));
		
		// python
		System.out.println("library(reticulate)" + re.eval("library(reticulate)"));
		//System.out.println("repl " + re.eval("repl_python()"));
		System.out.println("import" + re.eval("pd <- import(\"pandas\")"));
		System.out.println("pandas load " + re.eval("k <- pd$read_csv(\"c:/users/pkapaleeswaran/workspacej3/datasets/Movie.csv\")"));
		System.out.println("pandas load " + re.eval("k")); // <- pd.read_csv(\"c:/users/pkapaleeswaran/workspacej3/datasets/Movie.csv\")"));
		
		
		System.out.println("Loading the data" + re.eval("tx <- read.csv(\"c:/users/pkapaleeswaran/workspacej3/datasets/TxContracts.csv\");" ));
		System.out.println("Loading the data" + re.eval("tx"));

		// load the drivers
		System.out.println(" Load Driver...  " + re.eval("paste(capture.output(print(drv <- JDBC('org.h2.Driver', 'C:/Users/pkapaleeswaran/workspacej3/SemossWeb/RDFGraphLib/h2-1.4.185.jar', identifier.quote='`'),collapse='\n')"));
		System.out.println("Connection.. " + re.eval("paste(capture.output(print(conn <- dbConnect(drv, 'jdbc:h2:tcp://192.168.1.8:5355/mem:test:LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0', 'sa', ''),collapse='\n')"));;
		System.out.println("Create variable with table " + re.eval("paste(capture.output(print(dt <-as.data.table(unclass(dbGetQuery(conn,'SELECT * FROM H2FRAMEE7E0559A_04CF_43B7_BD04_7A3CCE7B3650'))), collapse='\n')"));
		System.out.println(re.eval("dt"));
		re.eval("names(airquality) <- tolower(names(airquality));");
		System.out.println("AIRQUALITY " + re.eval("airquality"));
		System.out.println(re.eval(".libpaths(.Library)"));
		System.out.println("Loading reshape" + re.eval("library(reshape2);"));
		System.out.println(" >> " + re.eval("aql <- melt(airquality);head(aql)"));
		
		//System.exit(1);
		//System.out.println("install >>" + re.eval("install.packages('randomForest',repos='http://cran.us.r-project.org');"));
		
        System.out.println("Rengine created, waiting for R");
		// the engine creates R is a new thread, so we should wait until it's ready
        if (!re.waitForR()) {
            System.out.println("Cannot load R");
            return;
        }

		/* High-level API - do not use RNI methods unless there is no other way
			to accomplish what you want */
		try {
			REXP x;
			re.eval("data(iris)",false);
			
			System.out.println(">>>>>>" + re.eval(" 2 + 3;").asDouble());
			System.out.println(x=re.eval("iris"));
			// generic vectors are RVector to accomodate names
			RVector v = x.asVector();
			if (v.getNames()!=null) {
				System.out.println("has names:");
				for (Enumeration e = v.getNames().elements() ; e.hasMoreElements() ;) {
					System.out.println(e.nextElement());
				}
			}
			// for compatibility with Rserve we allow casting of vectors to lists
			RList vl = x.asList();
			String[] k = vl.keys();
			if (k!=null) {
				System.out.println("and once again from the list:");
				int i=0; while (i<k.length) System.out.println(k[i++]);
			}			

			// get boolean array
			System.out.println(x=re.eval("iris[[1]]>mean(iris[[1]])"));
			// R knows about TRUE/FALSE/NA, so we cannot use boolean[] this way
			// instead, we use int[] which is more convenient (and what R uses internally anyway)
			int[] bi = x.asIntArray();
			{
			    int i = 0; while (i<bi.length) { System.out.print(bi[i]==0?"F ":(bi[i]==1?"T ":"NA ")); i++; }
			    System.out.println("");
			}
			
			// push a boolean array
			boolean by[] = { true, false, false };
			re.assign("bool", by);
			System.out.println(x=re.eval("bool"));
			// asBool returns the first element of the array as RBool
			// (mostly useful for boolean arrays of the length 1). is should return true
			System.out.println("isTRUE? "+x.asBool().isTRUE());

			// now for a real dotted-pair list:
			System.out.println(x=re.eval("pairlist(a=1,b='foo',c=1:5)"));
			RList l = x.asList();
			if (l!=null) {
				int i=0;
				String [] a = l.keys();
				System.out.println("Keys:");
				while (i<a.length) System.out.println(a[i++]);
				System.out.println("Contents:");
				i=0;
				while (i<a.length) System.out.println(l.at(i++));
			}
			System.out.println(re.eval("sqrt(36)"));
		} catch (Exception e) {
			System.out.println("EX:"+e);
			e.printStackTrace();
		}
		
		// Part 2 - low-level API - for illustration purposes only!
		//System.exit(0);
		
        // simple assignment like a<-"hello" (env=0 means use R_GlobalEnv)
        long xp1 = re.rniPutString("hello");
        re.rniAssign("a", xp1, 0);

        // Example: how to create a named list or data.frame
        double da[] = {1.2, 2.3, 4.5};
        double db[] = {1.4, 2.6, 4.2};
        long xp3 = re.rniPutDoubleArray(da);
        long xp4 = re.rniPutDoubleArray(db);
        
        // now build a list (generic vector is how that's called in R)
        long la[] = {xp3, xp4};
        long xp5 = re.rniPutVector(la);

        // now let's add names
        String sa[] = {"a","b"};
        long xp2 = re.rniPutStringArray(sa);
        re.rniSetAttr(xp5, "names", xp2);

        // ok, we have a proper list now
        // we could use assign and then eval "b<-data.frame(b)", but for now let's build it by hand:       
        String rn[] = {"1", "2", "3"};
        long xp7 = re.rniPutStringArray(rn);
        re.rniSetAttr(xp5, "row.names", xp7);
        
        long xp6 = re.rniPutString("data.frame");
        re.rniSetAttr(xp5, "class", xp6);
        
        // assign the whole thing to the "b" variable
        re.rniAssign("b", xp5, 0);
        
        {
            System.out.println("Parsing");
            long e=re.rniParse("data(iris)", 1);
            System.out.println("Result = "+e+", running eval");
            long r=re.rniEval(e, 0);
            System.out.println("Result = "+r+", building REXP");
            REXP x=new REXP(re, r);
            System.out.println("REXP result = "+x);
        }
        {
            System.out.println("Parsing");
            long e=re.rniParse("iris", 1);
            System.out.println("Result = "+e+", running eval");
            long r=re.rniEval(e, 0);
            System.out.println("Result = "+r+", building REXP");
            REXP x=new REXP(re, r);
            System.out.println("REXP result = "+x);
        }
        {
            System.out.println("Parsing");
            long e=re.rniParse("names(iris)", 1);
            System.out.println("Result = "+e+", running eval");
            long r=re.rniEval(e, 0);
            System.out.println("Result = "+r+", building REXP");
            REXP x=new REXP(re, r);
            System.out.println("REXP result = "+x);
            String s[]=x.asStringArray();
            if (s!=null) {
                int i=0; while (i<s.length) { System.out.println("["+i+"] \""+s[i]+"\""); i++; }
            }
        }
        {
            System.out.println("Parsing");
            long e=re.rniParse("rnorm(10)", 1);
            System.out.println("Result = "+e+", running eval");
            long r=re.rniEval(e, 0);
            System.out.println("Result = "+r+", building REXP");
            REXP x=new REXP(re, r);
            System.out.println("REXP result = "+x);
            double d[]=x.asDoubleArray();
            if (d!=null) {
                int i=0; while (i<d.length) { System.out.print(((i==0)?"":", ")+d[i]); i++; }
                System.out.println("");
            }
            System.out.println("");
        }
        {
            REXP x=re.eval("1:10");
            System.out.println("REXP result = "+x);
            int d[]=x.asIntArray();
            if (d!=null) {
                int i=0; while (i<d.length) { System.out.print(((i==0)?"":", ")+d[i]); i++; }
                System.out.println("");
            }
        }

        re.eval("print(1:10/3)");
        
	if (true) {
	    // so far we used R as a computational slave without REPL
	    // now we start the loop, so the user can use the console
	    System.out.println("Now the console is yours ... have fun");
	    re.startMainLoop();
	} else {
	    re.end();
	    System.out.println("end");
	}
    }
}
