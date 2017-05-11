package com.dominikcebula.hadoop.ftp;

import java.io.IOException;

import org.apache.ftpserver.ftplet.*;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

class HdfsFileSystemView implements FileSystemView
{
   private static final Logger LOGGER = Logger.getLogger(HdfsFileSystemView.class);
   private static final String SLASH = "/";
   private String currDir = SLASH;

   private User user;
   private FileSystem dfs;

   HdfsFileSystemView(User user, FileSystem fileSystem) throws FtpException
   {
      if (user == null)
      {
         LOGGER.error("User can not be null");
         throw new IllegalArgumentException("user can not be null");
      }
      if (user.getHomeDirectory() == null)
      {
         LOGGER.error("User home directory can not be null");
         throw new IllegalArgumentException("User home directory can not be null");
      }

      dfs = fileSystem;
      LOGGER.info("login hdfs user name = " + user.getName());

      try
      {
         if (!dfs.exists(new Path(user.getHomeDirectory())))
         {
            dfs.mkdirs(new Path(user.getHomeDirectory()));
         }
      }
      catch (IOException e)
      {
         throw new FtpException(e);
      }

      this.currDir = user.getHomeDirectory();
      this.user = user;
   }

   public FtpFile getHomeDirectory() throws FtpException
   {
      return new HdfsFtpFile(user.getHomeDirectory(), user, dfs);
   }

   public FtpFile getWorkingDirectory() throws FtpException
   {
      return new HdfsFtpFile(currDir, user, dfs);
   }

   public FtpFile getFile(String file) throws FtpException
   {
      String path;
      if (file.startsWith(SLASH))
      {
         if (file.startsWith(user.getHomeDirectory()))
         {
            path = file;
         }
         else
         {
            path = user.getHomeDirectory() + file;
         }
      }
      else if (currDir.length() > 1)
      {
         path = currDir + SLASH + file;
      }
      else
      {
         path = SLASH + file;
      }
      return new HdfsFtpFile(path, user, dfs);
   }

   public boolean changeWorkingDirectory(String dir) throws FtpException
   {
      String path;
      if (dir.startsWith(SLASH))
      {
         if (dir.startsWith(user.getHomeDirectory()))
         {
            path = dir;
         }
         else
         {
            path = user.getHomeDirectory() + dir;
         }

      }
      else if (dir.equals(".."))
      {
         if (!new Path(currDir).toString().equals(SLASH))
         {
            path = currDir + "/..";
         }
         else
         {
            path = SLASH;
         }
      }
      else if (dir.startsWith("~"))
      {
         path = user.getHomeDirectory() + dir.substring(1);
      }
      else if (currDir.length() > 1)
      {
         path = currDir + SLASH + dir;
      }
      else
      {
         path = SLASH + dir;
      }
      HdfsFtpFile file = new HdfsFtpFile(path, user, dfs);
      if (file.isDirectory() && file.isReadable() && new Path(path).toString().startsWith(user.getHomeDirectory()))
      {
         currDir = path;
      }
      else if (user.getHomeDirectory().startsWith(path))
      {
         currDir = user.getHomeDirectory();
      }
      return true;
   }

   public boolean isRandomAccessible()
   {
      return true;
   }

   public void dispose()
   {
   }
}
