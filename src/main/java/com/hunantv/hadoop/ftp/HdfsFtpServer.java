package com.hunantv.hadoop.ftp;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.ftpserver.ConnectionConfig;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
//import org.apache.ftpserver.filesystem.nativefs.NativeFileSystemFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.impl.DefaultConnectionConfig;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.log4j.Logger;


public class HdfsFtpServer 
{
	private static Logger log = Logger.getLogger(HdfsFtpServer.class);
	private static int port = 0;
	private static String userConf = null;
	
    private static int maxLogins = 50;
    private static boolean anonymousLoginEnabled = false;
    private static int maxAnonymousLogins = 0;
    private static int maxLoginFailures = 3;
    private static int loginFailureDelay = 500;
    private static int maxThreads = 200;
	
    public static void main( String[] args ) throws IOException, ParseException
    {
    	CommandLineParser parser = new BasicParser();  
    	Options options = new Options();
    	options.addOption("h", "help", false, "print help for the command." ); 
    	options.addOption("p", "port", true, "Set FTP server port" );  
    	options.addOption("u", "userconf", true, "Set FTP user configure file");  
    	options.addOption("t", "maxthreads", true, "Set FTP maximum number of threads used in the thread pool");  
    	options.addOption("l", "maxlogins", true, "Set FTP maximum number of simultaneous users login");  
    	CommandLine commandLine = parser.parse( options, args );  
    	if (commandLine.hasOption("h")) {
            HelpFormatter hf = new HelpFormatter();
            String formatstr = "hdfs-over-ftp [-h/--help] [-t/--maxthreads threads] [-l/--maxlogins logins] -p port -u userconf";
            hf.printHelp(formatstr, "", options, "");
            return;
        }
    	if( commandLine.hasOption('p') ) {  
    		port = Integer.parseInt(commandLine.getOptionValue('p'));
    		log.info("Set FTP server port = " + port);
    	}
    	if( commandLine.hasOption('u') ) {  
    		userConf = commandLine.getOptionValue('u');
    		log.info("Set FTP user configure file = " + userConf);
    	}
    	if( commandLine.hasOption('t') ) {
    		maxThreads = Integer.parseInt(commandLine.getOptionValue('t'));
    		log.info("Set FTP maximum number of threads = " + maxThreads);
    	}
    	if( commandLine.hasOption('l') ) {  
    		maxLogins = Integer.parseInt(commandLine.getOptionValue('l'));
    		log.info("Set FTP maximum number of simultaneous users login = " + maxLogins);
    	}
    	
    	if(port <= 0 || port >65535 || userConf == null || !(new File(userConf).exists())) {
    		log.error("Get port or user configure file error, Usage: hdfs-ftp -p port -u userconf -t threads -l logins");
    		return;
    	}

    	FtpServerFactory serverFactory = new FtpServerFactory();
    	ListenerFactory factory = new ListenerFactory();
    	factory.setPort(port);
    	serverFactory.addListener("default", factory.createListener());

    	PropertiesUserManagerFactory um = new PropertiesUserManagerFactory();
    	um.setFile(new File(userConf));
    	um.setPasswordEncryptor(new ClearTextPasswordEncryptor());
    	serverFactory.setUserManager(um.createUserManager());
    	
    	ConnectionConfig connConf = new DefaultConnectionConfig(anonymousLoginEnabled, loginFailureDelay, 
    			maxLogins, maxAnonymousLogins, maxLoginFailures, maxThreads);
    	serverFactory.setConnectionConfig(connConf);
    	log.info("FTP Connection configure infomation : anonymous login enabled = " + false + 
    			", login failure delay = " + loginFailureDelay + ", max users logins = " + maxLogins +
    			", max anonymous logins = " + maxAnonymousLogins + ", max login failures = " + maxLoginFailures + 
    			", max threads = " + maxThreads);
    	//serverFactory.setFileSystem(new NativeFileSystemFactory());
    	serverFactory.setFileSystem(new HdfsFileSystemFactory());
  
    	// start the server
    	FtpServer server = serverFactory.createServer(); 
    	log.info("Start FTP Server");
    	try {
			server.start();
		} catch (FtpException e) {
			e.printStackTrace();
		}
    }
}
