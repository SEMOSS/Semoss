package prerna.pyserve;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import jep.Jep;
import prerna.ds.py.PyExecutorThread;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class TCPPyWorker 
{
	
	// basically a process which works by looking for commands in TCP space
	private static final String CLASS_NAME = TCPPyWorker.class.getName();

	List <String> commandList = new ArrayList<String>(); // this is giving the file name and that too relative
	public static Logger LOGGER = LogManager.getLogger(CLASS_NAME);

	String internalLock = "Internal Lock";
	private static boolean first = true;
	public Jep jep = null;
	boolean SSL = false;
	
	public String [] command = null;
	public Hashtable <String, Object> response = new Hashtable<String, Object>();
	
	public volatile boolean keepAlive = true;
	public volatile boolean ready = false;
	public Object driverMonitor = null;
	
	Properties prop = null; // this is basically reference to the RDF Map
	List foldersBeingWatched = new ArrayList();
	public String mainFolder = null;
	PyExecutorThread pt = null;

	static boolean test = false;

	
	public static void main(String [] args)
	{
		
		
		// arg1 - the directory where commands would be thrown
		// arg2 - access to the rdf map to load
		// arg3 - port to start
		
		// create the watch service
		// start this thread
		
		// when event comes write it to the command
		// comment this for main execution
		//-Dlog4j.defaultInitOverride=TRUE
		
		if(args == null || args.length == 0)
		{
			args = new String[3];
			args[0] = "c:/users/pkapaleeswaran/workspacej3/SemossDev/InsightCache/keeper";
			args[1] = "c:/users/pkapaleeswaran/workspacej3/SemossDev/RDF_MAP.prop";
			args[2] = "8007";
			test = true;
		}
		
		String log4JPropFile = Paths.get(args[0], "log4j2.properties").toAbsolutePath().toString();
		
		FileInputStream fis = null;
		ConfigurationSource source = null;
		try {
			fis = new FileInputStream(log4JPropFile);
			source = new ConfigurationSource(fis);
			//Configuration con = PropertiesConfigurationFactory.getInstance().getConfiguration(new LoggerContext(CLASS_NAME), source);
			//LOGGER = con.getLoggerContext().getLogger(CLASS_NAME);
			//LOGGER = Configurator.initialize(null, source).getLogger(CLASS_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		TCPPyWorker worker = new TCPPyWorker();
		DIHelper.getInstance().loadCoreProp(args[1]);
		DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		worker.prop = new Properties();
		try {
			worker.prop.load(new FileInputStream(args[1]));
			System.out.println("Loaded the rdf map");
			
			// get the library for jep
			//String jepLib = worker.prop.getProperty("JEP_LIB");
			
			//System.loadLibrary(jepLib);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		worker.mainFolder = args[0];
		worker.bootServer(Integer.parseInt(args[2]));
	}
	
	public void startPyExecutor()
	{
		if(this.pt== null)
		{
			pt = new PyExecutorThread();
			//pt.getJep();
			pt.start();
			
			
			while(!pt.isAlive())
			{
				try {
					// sleep until we get the py
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			LOGGER.info("PyThread Started");
		}
	}
	
	public void bootServer(int PORT)
	{
        // Configure SSL.
        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            final SslContext sslCtx;
            if (SSL) {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
            } else {
                sslCtx = null;
            }

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 100)
//             .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, (1024*1024))
//             .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, (512*1024))
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc()));
                     }
                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     TCPServerHandler tsh = new TCPServerHandler();
                     tsh.setBossGroup(bossGroup);
                     tsh.setWorkerGroup(workerGroup);
                     tsh.setLogger(LOGGER);
                     tsh.setPyExecutorThread(pt);
                     tsh.setMainFolder(mainFolder);
                     tsh.setTest(test);
                     p
                     //.addLast(new LengthFieldPrepender(4))
                     .addLast(tsh);
                 }
             });
            // start python thread
    		startPyExecutor();

            
            // Start the server.
            ChannelFuture f = b.bind(PORT).sync();
            LOGGER.info("Listening on port " + PORT);
            LOGGER.info("set watermarks");
            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        }catch (IOException ex)
        {
        	LOGGER.debug("Connection Closed ");
        }catch(Exception ex)
        {
        	LOGGER.debug(ex);
        }
        finally 
        {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
		
	}

}
