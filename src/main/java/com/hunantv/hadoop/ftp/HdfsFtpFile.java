package com.hunantv.hadoop.ftp;

import java.io.*;
import java.util.*;

import org.apache.ftpserver.ftplet.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

class HdfsFtpFile implements FtpFile
{
   private static final String SLASH = "/";
   private final Logger log = Logger.getLogger(HdfsFtpFile.class);

   private final Path path;
   private final User user;
   private final FileSystem fileSystem;
   private FileStatus status;

   public HdfsFtpFile(String path, User user, FileSystem fileSystem) throws FtpException
   {
      this.path = new Path(path);
      this.user = user;
      this.fileSystem = fileSystem;

      tryInitializeFileStatus(fileSystem);
   }

   private void tryInitializeFileStatus(FileSystem dfs) throws FtpException
   {
      try
      {
         initializeFileStatus(dfs);
      }
      catch (IOException e)
      {
         throw new FtpException(e);
      }
   }

   private void initializeFileStatus(FileSystem dfs) throws IOException
   {
      if (dfs.exists(this.path))
      {
         this.status = dfs.getFileStatus(this.path);
      }
   }

   public String getAbsolutePath()
   {
      return path.toString();
   }

   public String getName()
   {
      String full = getAbsolutePath();
      int pos = full.lastIndexOf(SLASH);
      if (full.length() == 1)
      {
         return SLASH;
      }
      return full.substring(pos + 1);
   }

   public boolean isHidden()
   {
      return false;
   }

   public boolean isDirectory()
   {
      return status.isDirectory();
   }

   private FsPermission getPermissions() throws IOException
   {
      return fileSystem.getFileStatus(path).getPermission();
   }

   public boolean isFile()
   {
      return status.isFile();
   }

   public boolean doesExist()
   {
      return status != null;
   }

   public boolean isReadable()
   {
      try
      {
         FsPermission permissions = getPermissions();
         if (user.getName().equals(getOwnerName()))
         {
            if (permissions.toString().substring(0, 1).equals("r"))
            {
               log.debug("PERMISSIONS: " + path + " - " + " read allowed for user");
               return true;
            }
         }
         else if (user.equals(getGroupName()))
         {
            if (permissions.toString().substring(3, 4).equals("r"))
            {
               log.debug("PERMISSIONS: " + path + " - " + " read allowed for group");
               return true;
            }
         }
         else
         {
            if (permissions.toString().substring(6, 7).equals("r"))
            {
               log.debug("PERMISSIONS: " + path + " - " + " read allowed for others");
               return true;
            }
         }
         log.debug("PERMISSIONS: " + path + " - " + " read denied");
         return false;
      }
      catch (IOException e)
      {
         log.warn(e.getMessage(), e);
         return false;
      }
   }

   private HdfsFtpFile getParent() throws FtpException
   {
      String pathS = path.toString();
      String parentS = SLASH;
      int pos = pathS.lastIndexOf(SLASH);
      if (pos > 0)
      {
         parentS = pathS.substring(0, pos);
      }
      return new HdfsFtpFile(parentS, user, fileSystem);
   }

   public boolean isWritable()
   {
      try
      {
         FsPermission permissions = getPermissions();
         if (user.getName().equals(getOwnerName()))
         {
            if (permissions.toString().substring(1, 2).equals("w"))
            {
               log.debug("PERMISSIONS: " + path + " - " + " write allowed for user");
               return true;
            }
         }
         else if (user.equals(getGroupName()))
         {
            if (permissions.toString().substring(3, 4).equals("w"))
            {
               log.debug("PERMISSIONS: " + path + " - " + " read allowed for group");
               return true;
            }
         }
         else
         {
            if (permissions.toString().substring(7, 8).equals("w"))
            {
               log.debug("PERMISSIONS: " + path + " - " + " write allowed for others");
               return true;
            }
         }
         log.debug("PERMISSIONS: " + path + " - " + " write denied");
         return false;
      }
      catch (IOException ioException)
      {
         try
         {
            return getParent().isWritable();
         }
         catch (FtpException ftpException)
         {
            log.warn(ftpException.getMessage(), ftpException);
            return false;
         }
      }
   }

   public boolean isRemovable()
   {
      return isWritable();
   }

   public String getOwnerName()
   {
      if (status != null)
      {
         return status.getOwner();
      }
      try
      {
         FileStatus fs = fileSystem.getFileStatus(path);
         return fs.getOwner();
      }
      catch (IOException e)
      {
         log.warn(e.getMessage(), e);
         return null;
      }
   }

   public String getGroupName()
   {
      if (status != null)
      {
         return status.getGroup();
      }
      try
      {
         FileStatus fs = fileSystem.getFileStatus(path);
         return fs.getGroup();
      }
      catch (IOException e)
      {
         log.warn(e.getMessage(), e);
         return null;
      }
   }

   public int getLinkCount()
   {
      return isDirectory() ? 3 : 1;
   }

   public long getLastModified()
   {
      return status.getModificationTime();
   }

   public boolean setLastModified(long l)
   {
      return false;
   }

   public long getSize()
   {
      return status.getLen();
   }

   @Override
   public Object getPhysicalFile()
   {
      return null;
   }

   public boolean mkdir()
   {
      if (!isWritable())
      {
         log.debug("No write permission : " + path);
         return false;
      }
      try
      {
         fileSystem.mkdirs(path);
         return true;
      }
      catch (IOException e)
      {
         log.error("mkdir error", e);
         return false;
      }
   }

   public boolean delete()
   {
      if (!isWritable())
      {
         log.debug("No write permission : " + path);
         return false;
      }
      try
      {
         fileSystem.delete(path, true);
         return true;
      }
      catch (IOException e)
      {
         log.error("delete error", e);
         return false;
      }
   }

   public boolean move(FtpFile fileObject)
   {
      if (!isWritable() && fileObject.isWritable())
      {
         log.debug("No write permission : " + path);
         return false;
      }
      try
      {
         fileSystem.rename(path, new Path(fileObject.getAbsolutePath()));
         return true;
      }
      catch (IOException e)
      {
         log.error("move error", e);
         return false;
      }
   }

   public List<FtpFile> listFiles()
   {
      if (!isReadable())
      {
         log.debug("No read permission : " + path);
         return null;
      }

      try
      {
         FileStatus fileStats[] = fileSystem.listStatus(path);

         FtpFile fileObjects[] = new HdfsFtpFile[fileStats.length];
         for (int i = 0; i < fileStats.length; i++)
         {
            fileObjects[i] = new HdfsFtpFile(fileStats[i].getPath().toString(), user, fileSystem);
         }
         return Arrays.asList(fileObjects);
      }
      catch (IOException | FtpException e)
      {
         log.debug("", e);
         return null;
      }
   }

   public OutputStream createOutputStream(long offset) throws IOException
   {
      if (!isWritable())
      {
         throw new IOException("No write permission : " + path);
      }

      try
      {
         FSDataOutputStream out;
         if (fileSystem.exists(path) && offset > 0)
         {
            out = fileSystem.append(path);
         }
         else
         {
            out = fileSystem.create(path);
         }
         return out;
      }
      catch (IOException e)
      {
         log.error("createOutputStream error", e);
         return null;
      }
   }

   public InputStream createInputStream(long offset) throws IOException
   {
      if (!isReadable())
      {
         throw new IOException("No read permission : " + path);
      }
      try
      {
         FSDataInputStream in = fileSystem.open(path);
         this.status = fileSystem.getFileStatus(path);
         if (offset < this.status.getLen())
         {
            in.seek(offset);
         }
         return in;
      }
      catch (IOException e)
      {
         log.error("createInputStream error", e);
         return null;
      }
   }
}

