package org.gbif.watchdog.config;

import org.gbif.ws.client.filter.HttpGbifAuthFilter;
import org.gbif.ws.client.guice.GbifApplicationAuthModule;
import org.gbif.ws.security.GbifAuthService;

import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.sun.jersey.api.client.filter.ClientFilter;

/**
 * Configures authentication for GBIF API.
 */
public class PrivateWatchdogModule extends PrivateModule {
  private final Properties properties;

  public PrivateWatchdogModule(Properties properties) {
    this.properties = properties;
  }

  @Override
  protected void configure() {
    Names.bindProperties(binder(), properties);

    bind(SessionAuthProvider.class).in(Scopes.SINGLETON);

    expose(ClientFilter.class);
  }

  @Provides
  @Singleton
  public GbifAuthService provideGbifAuthService() {
    String appKey = properties.getProperty(GbifApplicationAuthModule.PROPERTY_APP_KEY);
    String appSecret = properties.getProperty(GbifApplicationAuthModule.PROPERTY_APP_SECRET);

    return GbifAuthService.singleKeyAuthService(appKey, appSecret);
  }

  @Provides
  @Singleton
  @Inject
  public ClientFilter provideSessionAuthFilter(GbifAuthService authService, SessionAuthProvider sessionAuthProvider) {
    return new HttpGbifAuthFilter(authService, sessionAuthProvider);
  }
}
