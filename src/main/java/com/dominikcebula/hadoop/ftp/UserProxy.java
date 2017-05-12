package com.dominikcebula.hadoop.ftp;

import java.util.List;

import org.apache.ftpserver.ftplet.*;

public class UserProxy implements User
{
   private final User user;

   UserProxy(User user)
   {
      this.user = user;
   }

   @Override
   public String getName()
   {
      return user.getName();
   }

   @Override
   public String getPassword()
   {
      return user.getPassword();
   }

   @Override
   public List<? extends Authority> getAuthorities()
   {
      return user.getAuthorities();
   }

   @Override
   public List<? extends Authority> getAuthorities(Class<? extends Authority> aClass)
   {
      return user.getAuthorities(aClass);
   }

   @Override
   public AuthorizationRequest authorize(AuthorizationRequest authorizationRequest)
   {
      return user.authorize(authorizationRequest);
   }

   @Override
   public int getMaxIdleTime()
   {
      return user.getMaxIdleTime();
   }

   @Override
   public boolean getEnabled()
   {
      return user.getEnabled();
   }

   @Override
   public String getHomeDirectory()
   {
      return user.getHomeDirectory();
   }

   public static class UserProxyFactory
   {
      public UserProxy createUserProxy(User user)
      {
         return new UserProxy(user);
      }
   }
}
