package prerna.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import com.amazon.support.CustomClassLoader;

public class SemossClassloader extends ClassLoader {
	
	String folder = null;
	Map <String, Boolean> dbLoaded = new Hashtable<String, Boolean>();
	
	public void setFolder(String folder)
	{
		this.folder = folder;
	}
	
	public SemossClassloader(ClassLoader parent)
	{
		super(parent);
	}
	
	public void commitEngine(String engine)
	{
		dbLoaded.put(engine, true);
	}
	
	public void uncommitEngine(String engine)
	{
		dbLoaded.remove(engine);
	}
	
	public boolean isCommitted(String engine)
	{
		return dbLoaded.containsKey(engine);
	}

	/**
     * Loads a given class from .class file just like
     * the default ClassLoader. This method could be
     * changed to load the class over network from some
     * other server or from the database.
     *
     * @param name Full class name
     */
    private Class<?> getClass(String name)
        throws ClassNotFoundException {
        // We are getting a name that looks like
        // javablogging.package.ClassToLoad
        // and we have to convert it into the .class file name
        // like javablogging/package/ClassToLoad.class
        String file = name.replace('.', File.separatorChar)
            + ".class";
        byte[] b = null;
        try {
            // This loads the byte code data from the file
            b = loadClassData(file);
            // defineClass is inherited from the ClassLoader class
            // and converts the byte array into a Class
            Class<?> c = defineClass(name, b, 0, b.length);
            resolveClass(c);
            return c;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
 
     /**
     * Every request for a class passes through this method.
     * If the requested class is in "javablogging" package,
     * it will load it using the
     * {@link CustomClassLoader#getClass()} method.
     * If not, it will use the super.loadClass() method
     * which in turn will pass the request to the parent.
     *
     * @param name
     *            Full class name
     */
    @Override
    public Class<?> loadClass(String name)
        throws ClassNotFoundException {
        Class retClass = null;
        // see if it is already loaded or in the classpath
        try
        {
        	retClass = super.loadClass(name);
        }catch(Exception ex)
        {
        	
        }
        
        if (retClass == null) 
        {
        	System.err.println("App Specific Class " + name);
            retClass = getClass(name);
        }
        return retClass;
    }
 
     /**
     * Loads a given file (presumably .class) into a byte array.
     * The file should be accessible as a resource, for example
     * it could be located on the classpath.
     * 
     * TODO: Need to incorporate loading jars. Not right now
     *
     * @param name File name to load
     * @return Byte array read from the file
     * @throws IOException Is thrown when there
     *               was some problem reading the file
     */
    private byte[] loadClassData(String name) throws IOException {
    	//System.out.println("Loading class data " + name);
    	
    	/*
        // Opening the file
        InputStream stream = getClass().getClassLoader()
            .getResourceAsStream(name);
        int size = stream.available();
        byte buff[] = new byte[size];
        DataInputStream in = new DataInputStream(stream);
        // Reading the binary data
        in.readFully(buff);
        in.close();
        return buff;
        */
        // Opening the file
    	FileInputStream stream = new FileInputStream(new File(folder + "/" + name));
        int size = stream.available();
        byte buff[] = new byte[size];
        DataInputStream in = new DataInputStream(stream);
        // Reading the binary data
        in.readFully(buff);
        in.close();
        return buff;
    }
    
    
    protected Class	findClass(String name) throws ClassNotFoundException
    {
    	//System.out.println("Find class called " + name);
		return super.findClass(name);
    	
    }
	
}
