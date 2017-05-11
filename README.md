hdfs-ftp
========
HDFS-FTP is a library which provides Apache MINA FileSystemView implementation for HDFS.
This allows you to easily create FTP over HDFS server embedded into your project.

This project is based on original: https://github.com/heguangwu/hdfs-ftp

Library should be used with serverFactory in following way:
```java
FileSystem fileSystem = getFileSystem();	// getFileSystem() method has to be implemented
FtpServerFactory serverFactory = new FtpServerFactory();
serverFactory.setFileSystem(new HdfsFileSystemFactory(fileSystem));

FtpServer server = serverFactory.createServer();
server.start();
```