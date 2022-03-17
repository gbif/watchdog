package org.gbif;

import freemarker.template.TemplateException;
import org.gbif.api.model.registry.*;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.NodeType;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Optional;
import java.util.UUID;

/**
 * Class rescues orphan datasets by retrieving them from the crawler cache (well, not really, just updating the registry).
 */
public class DatasetRescueFromXmlCache {

  private static Logger LOG = LoggerFactory.getLogger(DatasetRescueFromXmlCache.class);

  DatasetService datasetService;
  OrganizationService organizationService;
  NodeService nodeService;

  DatasetRescueFromXmlCache(DatasetService datasetService, OrganizationService organizationService, NodeService nodeService) throws IOException {
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;
  }

  /**
   * Downloads a dataset from GBIF.org in DwC-A format using its GBIF datasetKey.
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(String datasetKey)
    throws IOException, ParserConfigurationException, SAXException, TemplateException, NoSuchFieldException,
    InterruptedException, URISyntaxException {

    UUID uuid = UUID.fromString(datasetKey);
    Dataset dataset = datasetService.get(uuid);
    Organization organization = organizationService.get(dataset.getPublishingOrganizationKey());
    Node node = nodeService.get(organization.getEndorsingNodeKey());

    String status = datasetService.listMachineTags(uuid).stream()
        .filter(mt -> mt.getNamespace().equals("orphans.gbif.org") && mt.getName().equals("status"))
        .findFirst()
        .map(mt -> mt.getValue())
        .orElse(null);

    if (status != null) {
      LOG.error("Dataset {} already rescued, status {}", datasetKey, status);
      System.exit(1);
    }

    File dwca = new File("./"+datasetKey+".zip");
    if (!dwca.exists()) {
      LOG.error("Dataset {} ZIP file doesn't exist at {}", datasetKey, dwca);
      System.exit(2);
    }

    MachineTag cacheRescue = new MachineTag();
    cacheRescue.setNamespace("orphans.gbif.org");
    cacheRescue.setName("crawlerArchiveCacheTime");
    cacheRescue.setValue(""+dwca.lastModified());
    datasetService.addMachineTag(uuid, cacheRescue);

    MachineTag orphanExport = new MachineTag();
    orphanExport.setNamespace("orphans.gbif.org");
    orphanExport.setName("status");
    orphanExport.setValue("RESCUED");
    datasetService.addMachineTag(uuid, orphanExport);

    MachineTag orphanEndpoint = new MachineTag();
    orphanEndpoint.setNamespace("orphans.gbif.org");
    orphanEndpoint.setName("orphanEndpoint");
    orphanEndpoint.setValue(dataset.getEndpoints().get(0).getUrl().toString());
    datasetService.addMachineTag(uuid, orphanEndpoint);

    // Update endpoint in the registry
    Optional<Endpoint> oldEndpoint = dataset.getEndpoints().stream()
        .filter(ep -> ep.getType() == EndpointType.BIOCASE_XML_ARCHIVE)
        .findFirst();

    if (!oldEndpoint.isPresent() || !oldEndpoint.get().getUrl().toString().contains("orphans.gbif.org")) {
      if (oldEndpoint.isPresent()) {
        datasetService.deleteEndpoint(uuid, oldEndpoint.get().getKey());
      }

      String endPointDirectory = null;

      if (node.getType() == NodeType.COUNTRY) {
        endPointDirectory = organization.getCountry().getIso2LetterCode().toUpperCase();
      } else {
        endPointDirectory = organization.getEndorsingNodeKey().toString();
      }

      Endpoint newEndpoint = new Endpoint();
      newEndpoint.setType(EndpointType.BIOCASE_XML_ARCHIVE);
      newEndpoint.setUrl(new URI("https://orphans.gbif.org/" + endPointDirectory + "/" + datasetKey + ".zip"));
      newEndpoint.setDescription("Orphaned dataset awaiting adoption.");
      datasetService.addEndpoint(uuid, newEndpoint);
    }
  }

  public static void main(String... args)
    throws ParseException, IOException, ParserConfigurationException, SAXException, TemplateException,
    NoSuchFieldException, InterruptedException, URISyntaxException {
    WatchdogModule watchdogModule = new WatchdogModule();
    DatasetRescueFromXmlCache rescuer = new DatasetRescueFromXmlCache(watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(), watchdogModule.setupNodeService());

    if (args.length < 1) {
      System.err.println("Give dataset keys as argument");
      System.exit(1);
    }

    for (String datasetKey : args) {
      rescuer.rescue(datasetKey);
    }
  }
}
