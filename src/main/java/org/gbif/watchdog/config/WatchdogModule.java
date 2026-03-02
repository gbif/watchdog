package org.gbif.watchdog.config;

import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.occurrence.ws.client.OccurrenceDownloadWsClient;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.DatasetProcessStatusClient;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.registry.ws.client.NodeClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.registry.ws.client.OrganizationClient;
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
   * Sets up an http client with a one-minute timeout and http support only.
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

  /**
   * Sets up a OccurrenceDownloadService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one-minute timeout and http support only.
   */
  public OccurrenceDownloadService setupOccurrenceDownloadService() {
    return clientBuilder.build(OccurrenceDownloadClient.class);
  }

  public DownloadRequestService setupDownloadRequestService() {
    return clientBuilder.build(OccurrenceDownloadWsClient.class);
  }
}
