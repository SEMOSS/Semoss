package prerna.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A custom class loader for dynamically loading project-specific classes in
 * Semoss. This class loader allows the application to load compiled Java
 * classes (.class files) from a specified folder that is not on the default
 * application classpath. This is essential for loading custom reactors and
 * other project-specific code at runtime.
 */
public class SemossClassloader extends ClassLoader {

	private static final Logger classLogger = LogManager.getLogger(SemossClassloader.class);

	private String folder = null;

	/**
	 * Constructs a new SemossClassloader with a specified parent class loader.
	 * 
	 * @param parent The parent class loader.
	 */
	public SemossClassloader(ClassLoader parent) {
		super(parent);
	}

	/**
	 * Sets the base folder from which to load .class files.
	 * 
	 * @param folder The absolute path to the directory containing the compiled
	 *               classes.
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}

	/**
	 * Finds and loads the class from a .class file. This method is called when a
	 * class is not found in the parent class loader's path.
	 *
	 * @param name The fully qualified name of the class to load.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found.
	 */
	private Class<?> getClass(String name) throws ClassNotFoundException {
		// We are getting a name that looks like
		// prerna.project.MyCustomClass
		// and we have to convert it into the .class file name
		// like prerna/project/MyCustomClass.class
		String file = name.replace('.', File.separatorChar) + ".class";
		byte[] b = null;
		try {
			// This loads the byte code data from the file
			b = loadClassData(file);
			// defineClass is inherited from the ClassLoader class
			// and converts the byte array into a Class
			if (b != null) {
				Class<?> c = defineClass(name, b, 0, b.length);
				resolveClass(c);
				return c;
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	/**
	 * Overrides the default class loading strategy. It first attempts to load the
	 * class using the parent class loader (the standard behavior). If the class is
	 * not found, it falls back to loading it from the custom folder location
	 * specified for this loader.
	 *
	 * @param name The fully qualified name of the class.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found in either the
	 *                                parent loader or the custom path.
	 */
	@Override
	public Class<?> loadClass(String name) throws ClassNotFoundException {
		Class retClass = null;
		// see if it is already loaded or in the classpath
		try {
			retClass = super.loadClass(name);
		} catch (Exception e) {
			// ignore
			// classLogger.error(Constants.STACKTRACE, e);
		}

		if (retClass == null) {
			classLogger.info("Project Specific Class " + name);
			retClass = getClass(name);
		}
		return retClass;
	}

	/**
	 * Reads the raw bytecode of a class file from the custom folder location.
	 * 
	 * TODO: Need to incorporate loading jars. Not right now
	 *
	 * @param name The relative path of the .class file to load.
	 * @return A byte array containing the class data.
	 * @throws IOException If there is a problem reading the file.
	 */
	private byte[] loadClassData(String name) throws IOException {
		FileInputStream stream = null;
		DataInputStream in = null;
		byte buff[] = null;
		try {
			stream = new FileInputStream(new File(folder + "/" + name));
			int size = stream.available();
			buff = new byte[size];
			in = new DataInputStream(stream);
			// Reading the binary data
			in.readFully(buff);
		} finally {
			try {
				if (stream != null) {
					stream.close();
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return buff;
	}

	/**
	 * Finds the specified class. This method is overridden from ClassLoader. In
	 * this implementation, it delegates to the parent's findClass method.
	 *
	 * @param name The fully qualified name of the class.
	 * @return The resulting Class object.
	 * @throws ClassNotFoundException If the class could not be found.
	 */
	@Override
	protected Class findClass(String name) throws ClassNotFoundException {
		return super.findClass(name);
	}

}
