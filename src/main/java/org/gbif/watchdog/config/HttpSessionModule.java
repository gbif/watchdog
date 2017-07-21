package org.gbif.watchdog.config;

import org.gbif.api.model.common.User;

import java.util.Enumeration;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * Provides an HttpSession with logged in GBIF.org user, which is used to execute all downloads.
 */
public class HttpSessionModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  public HttpSession provideHttpSession() {
    return new HttpSession() {
      @Override
      public long getCreationTime() {
        return 0;
      }

      @Override
      public String getId() {
        return null;
      }

      @Override
      public long getLastAccessedTime() {
        return 0;
      }

      @Override
      public ServletContext getServletContext() {
        return null;
      }

      @Override
      public void setMaxInactiveInterval(int interval) {

      }

      @Override
      public int getMaxInactiveInterval() {
        return 0;
      }

      @Override
      public HttpSessionContext getSessionContext() {
        return null;
      }

      @Override
      public Object getAttribute(String name) {
        if (name.equals(SessionAuthProvider.SESSION_USER)) {
          User user = new User();
          user.setEmail("kbraak@gbif.org");
          user.setFirstName("Kyle");
          user.setLastName("Braak");
          user.setUserName("Kyle Braak");
          user.setPasswordHash("password");
          return user;
        }
        return null;
      }

      @Override
      public Object getValue(String name) {
        return null;
      }

      @Override
      public Enumeration<String> getAttributeNames() {
        return null;
      }

      @Override
      public String[] getValueNames() {
        return new String[0];
      }

      @Override
      public void setAttribute(String name, Object value) {

      }

      @Override
      public void putValue(String name, Object value) {

      }

      @Override
      public void removeAttribute(String name) {

      }

      @Override
      public void removeValue(String name) {

      }

      @Override
      public void invalidate() {

      }

      @Override
      public boolean isNew() {
        return false;
      }
    };
  }
}
