package com.dominikcebula.hadoop.ftp;

import java.io.IOException;

import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsUserProxy extends UserProxy
{
   private HdfsUserProxy(User user)
   {
      super(user);
   }

   @Override
   public String getName()
   {
      try
      {
         return UserGroupInformation.getCurrentUser().getUserName();
      }
      catch (IOException e)
      {
         throw new RuntimeException(e);
      }
   }

   public static class HdfsUserProxyFactory extends UserProxyFactory
   {
      @Override
      public UserProxy createUserProxy(User user)
      {
         return new HdfsUserProxy(user);
      }
   }
}
