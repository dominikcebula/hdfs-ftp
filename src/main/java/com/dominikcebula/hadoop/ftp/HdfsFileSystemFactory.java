package com.dominikcebula.hadoop.ftp;

import org.apache.ftpserver.ftplet.*;
import org.apache.hadoop.fs.FileSystem;

public class HdfsFileSystemFactory implements FileSystemFactory
{
   private final FileSystem fileSystem;

   public HdfsFileSystemFactory(FileSystem fileSystem)
   {
      this.fileSystem = fileSystem;
   }

   public FileSystemView createFileSystemView(User user) throws FtpException
   {
      return new HdfsFileSystemView(user, fileSystem);
   }
}
