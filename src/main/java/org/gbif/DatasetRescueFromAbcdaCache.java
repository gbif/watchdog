package org.gbif;

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.Node;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.NodeType;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Retrieves archive crawl data from the cache, copies it to a webserver, adds machine tags and updates the endpoint.
 */
public class DatasetRescueFromAbcdaCache {

  private static Logger LOG = LoggerFactory.getLogger(DatasetRescueFromAbcdaCache.class);

  private final DatasetService datasetService;
  private final OrganizationService organizationService;
  private final NodeService nodeService;
  private final DatasetProcessStatusService statusService;

  DatasetRescueFromAbcdaCache(DatasetService datasetService, OrganizationService organizationService, NodeService nodeService, DatasetProcessStatusService statusService) throws IOException {
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;
    this.statusService = statusService;
  }

  private Instant getFileModTime(String datasetKey, int attempt) {
    assert datasetKey != null;
    assert attempt >= 1;

    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    processBuilder.command("ssh", "crap@cli1.gbif.org",
      "stat", "--dereference", "--format", "%Y", "storage/abcda/"+datasetKey+"."+attempt+".abcda");
    try {
      Process process = processBuilder.start();
      String result = new String(process.getInputStream().readAllBytes());
      int exitVal = process.waitFor();
      if (exitVal == 0) {
        Instant modTime = Instant.ofEpochSecond(Long.parseLong(result.trim()));
        return modTime;
      }
      LOG.error("Failed to read {}.{}.abcda", datasetKey, attempt);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  private boolean copyCachedArchive(String datasetKey, int attempt, String subdir) {
    assert datasetKey != null;
    assert attempt >= 1;
    assert subdir.length() >= 2;

    ProcessBuilder mkdirProcessBuilder = new ProcessBuilder();
    mkdirProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    mkdirProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    mkdirProcessBuilder.command("ssh", "mblissett@orphans.gbif.org",
      "mkdir", "-p", "/var/www/html/orphans.gbif.org/"+subdir);

    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.command("scp", "-p",
      "crap@cli1.gbif.org:storage/abcda/"+datasetKey+"."+attempt+".abcda",
      "mblissett@orphans.gbif.org:/var/www/html/orphans.gbif.org/"+subdir+"/"+datasetKey+"."+attempt+".zip");

    try {
      Process mkdirProcess = mkdirProcessBuilder.start();
      mkdirProcess.waitFor();
      int mkdirExitVal = mkdirProcess.waitFor();
      if (mkdirExitVal != 0) {
        LOG.error("Failed to mkdir {}", subdir);
        return false;
      }

      Process process = processBuilder.start();
      int exitVal = process.waitFor();
      if (exitVal == 0) {
        LOG.info("Copied {}.{}.abcda to https://orphans.gbif.org/{}/{}.{}.zip", datasetKey, attempt, subdir, datasetKey, attempt);
        return true;
      } else {
        LOG.error("Failed to copy storage/abcda/{}.{}.abcda to https://orphans.gbif.org/{}/{}.{}.zip, scp exited with {}", datasetKey, attempt, subdir, datasetKey, attempt, exitVal);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Retrieves a dataset from the crawler cache and updates the registry.
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(UUID datasetKey, int attempt) throws URISyntaxException {

    Dataset dataset = datasetService.get(datasetKey);

    if (dataset.getType() == DatasetType.METADATA) {
      LOG.info("Dataset {} is metadata-only, skipping", datasetKey);
      return;
    }

    Optional<Endpoint> oldEndpoint = dataset.getEndpoints().stream()
      .filter(ep -> ep.getType() == EndpointType.BIOCASE_XML_ARCHIVE)
      .findFirst();

    if (oldEndpoint.isEmpty()) {
      LOG.info("{} Can't rescue a non-ABCDA dataset from the AbcdaCache.", datasetKey);
      return;
    }

    Organization organization = organizationService.get(dataset.getPublishingOrganizationKey());
    Node node = nodeService.get(organization.getEndorsingNodeKey());

    String status = datasetService.listMachineTags(datasetKey).stream()
      .filter(mt -> mt.getNamespace().equals("orphans.gbif.org") && mt.getName().equals("status"))
      .findFirst()
      .map(mt -> mt.getValue())
      .orElse(null);

    if (status != null) {
      LOG.error("Dataset {} already rescued, status {}", datasetKey, status);
      return;
    }

    final String endPointDirectory;
    if (node.getType() == NodeType.COUNTRY) {
      endPointDirectory = organization.getCountry().getIso2LetterCode().toUpperCase();
    } else {
      endPointDirectory = organization.getEndorsingNodeKey().toString();
    }

    Instant modTime = getFileModTime(datasetKey.toString(), attempt);
    if (modTime == null) {
      LOG.error("Can't read file");
      return;
    }

    if (!copyCachedArchive(datasetKey.toString(), attempt, endPointDirectory)) {
      LOG.error("Copy failed");
      return;
    }

    MachineTag cacheRescue = new MachineTag();
    cacheRescue.setNamespace("orphans.gbif.org");
    cacheRescue.setName("crawlerAbcdaCacheTime");
    cacheRescue.setValue(DateTimeFormatter.ISO_INSTANT.format(modTime));
    datasetService.addMachineTag(datasetKey, cacheRescue);

    MachineTag orphanExport = new MachineTag();
    orphanExport.setNamespace("orphans.gbif.org");
    orphanExport.setName("status");
    orphanExport.setValue("RESCUED");
    datasetService.addMachineTag(datasetKey, orphanExport);

    MachineTag orphanEndpoint = new MachineTag();
    orphanEndpoint.setNamespace("orphans.gbif.org");
    orphanEndpoint.setName("orphanEndpoint");
    orphanEndpoint.setValue(dataset.getEndpoints().stream()
      .filter(ep -> ep.getType() == EndpointType.BIOCASE_XML_ARCHIVE)
      .findFirst().get().getUrl().toString());
    datasetService.addMachineTag(datasetKey, orphanEndpoint);

    // Update endpoint in the registry
    if (oldEndpoint.isEmpty() || !oldEndpoint.get().getUrl().toString().contains("orphans.gbif.org")) {
      datasetService.deleteEndpoint(datasetKey, oldEndpoint.get().getKey());

      Endpoint newEndpoint = new Endpoint();
      newEndpoint.setType(EndpointType.BIOCASE_XML_ARCHIVE);
      newEndpoint.setUrl(new URI("https://orphans.gbif.org/" + endPointDirectory + "/" + datasetKey + "." + attempt + ".zip"));
      newEndpoint.setDescription("Orphaned dataset awaiting adoption.");
      datasetService.addEndpoint(datasetKey, newEndpoint);
    }

    LOG.info("Completed rescue for https://registry.gbif.org/dataset/{}", datasetKey);
  }

  public static void main(String... args) throws Exception {
    WatchdogModule watchdogModule = new WatchdogModule();

    DatasetRescueFromAbcdaCache rescuer = new DatasetRescueFromAbcdaCache(watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(), watchdogModule.setupNodeService(), watchdogModule.setupDatasetProcessStatusService());

    List<Pair<String, Integer>> datasets = new ArrayList<>();

//    datasets.add(Pair.of("0c14c8bb-8d89-46f6-b0ee-6839b73a0c99", 57));

    for (Pair<String, Integer> dataset : datasets) {
      LOG.info("Rescuing {}", dataset.getLeft());
      rescuer.rescue(UUID.fromString(dataset.getLeft()), dataset.getRight());
    }
  }
}
