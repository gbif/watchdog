package org.gbif.watchdog.config;

import org.gbif.api.service.checklistbank.DatasetMetricsService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.*;
import org.gbif.checklistbank.ws.client.DatasetMetricsClient;
import org.gbif.occurrence.ws.client.OccurrenceWsSearchClient;
import org.gbif.registry.ws.client.*;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.util.Properties;

public class WatchdogModule {

  public static final String APPLICATION_PROPERTIES = "application.properties";

  private final ClientBuilder clientBuilder;

  /**
   * Constructs an instance using properties class instance.
   */
  public WatchdogModule(Properties properties) {
    clientBuilder = new ClientBuilder()
      .withUrl(properties.getProperty("api.url"))
      .withCredentials(properties.getProperty("gbif.user"), properties.getProperty("gbif.password"))
      .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
      .withFormEncoder();
  }

  private static Properties loadConfig() {
    try {
      return PropertiesUtil.loadProperties(APPLICATION_PROPERTIES);
    } catch (IOException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  /**
   * Constructs an instance using the default properties file.
   */
  public WatchdogModule() {
    this(loadConfig());
  }

  /**
   * Sets up a registry DatasetService client avoiding the use of guice as our gbif jackson libraries clash with the
   * hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public DatasetService setupDatasetService() {
    return clientBuilder.build(DatasetClient.class);
  }

  public OrganizationService setupOrganizationService() {
    return clientBuilder.build(OrganizationClient.class);
  }

  public NodeService setupNodeService() {
    return clientBuilder.build(NodeClient.class);
  }

  public DatasetProcessStatusService setupDatasetProcessStatusService() {
    return clientBuilder.build(DatasetProcessStatusClient.class);
  }

  public InstallationService setupInstallationService() {
    return clientBuilder.build(InstallationClient.class);
  }

  public DatasetMetricsService setupDatasetMetricsService() {
    return clientBuilder.build(DatasetMetricsClient.class);
  }

  public OccurrenceSearchService setupOccurrenceSearchService() {
    return clientBuilder.build(OccurrenceWsSearchClient.class);
  }

  /**
   * Sets up a OccurrenceDownloadService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public OccurrenceDownloadService setupOccurrenceDownloadService() {
    return clientBuilder.build(OccurrenceDownloadClient.class);
  }
}
