package org.gbif.watchdog.config;

import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.metrics.ws.client.guice.MetricsWsClientModule;
import org.gbif.occurrence.ws.client.OccurrenceWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.AbstractModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;

public class WatchdogModule extends AbstractModule {

  public static final String APPLICATION_PROPERTIES = "application.properties";

  @Override
  protected void configure() {
    try {
      Properties properties = PropertiesUtil.loadProperties(APPLICATION_PROPERTIES);

      // configure GBIF API authentication
      install(new SingleUserAuthModule(properties.getProperty("gbif.user"), properties.getProperty("gbif.password")));

      // provide GBIF.org authentication
      install(new HttpSessionModule());

      // bind registry service
      install(new RegistryWsClientModule(properties));

      // bind occurrence service
      install(new OccurrenceWsClientModule(properties));

      // bind metrics service
      install(new MetricsWsClientModule(properties));

      // bind checklistbank service
      install(new ChecklistBankWsClientModule(properties, true, true));
    } catch (IllegalArgumentException e) {
      this.addError(e);
    } catch (IOException e) {
      this.addError(e);
    }
  }

}
