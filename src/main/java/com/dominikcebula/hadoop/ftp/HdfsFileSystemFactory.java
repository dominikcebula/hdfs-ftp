package com.dominikcebula.hadoop.ftp;

import org.apache.ftpserver.ftplet.*;
import org.apache.hadoop.fs.FileSystem;

import static com.dominikcebula.hadoop.ftp.UserProxy.UserProxyFactory;

public class HdfsFileSystemFactory implements FileSystemFactory
{
   private final FileSystem fileSystem;
   private UserProxyFactory userProxyFactory;

   public HdfsFileSystemFactory(FileSystem fileSystem)
   {
      this(fileSystem, new UserProxyFactory());
   }

   public HdfsFileSystemFactory(FileSystem fileSystem, UserProxyFactory userProxyFactory)
   {
      this.fileSystem = fileSystem;
      this.userProxyFactory = userProxyFactory;
   }

   public FileSystemView createFileSystemView(User user) throws FtpException
   {
      return new HdfsFileSystemView(
         userProxyFactory.createUserProxy(user),
         fileSystem
      );
   }
}
