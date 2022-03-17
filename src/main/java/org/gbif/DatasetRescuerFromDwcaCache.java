package org.gbif;

import freemarker.template.TemplateException;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.registry.*;
import org.gbif.api.service.registry.DatasetProcessStatusService;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.gbif.api.model.crawler.FinishReason.NORMAL;

/**
 * Class rescues orphan datasets by retrieving them from the crawler cache.
 */
public class DatasetRescuerFromDwcaCache {

  private static Logger LOG = LoggerFactory.getLogger(DatasetRescuerFromDwcaCache.class);

  private static final int PAGING_LIMIT = 100;

  private final DatasetService datasetService;
  private final OrganizationService organizationService;
  private final NodeService nodeService;
  private final DatasetProcessStatusService statusService;

  DatasetRescuerFromDwcaCache(DatasetService datasetService, OrganizationService organizationService, NodeService nodeService, DatasetProcessStatusService statusService) throws IOException {
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;
    this.statusService = statusService;
  }

  private void findFinalCachedArchive(String datasetKey) {
    Dataset d = datasetService.get(UUID.fromString(datasetKey));

    // iterate through the dataset's crawl history
    PagingResponse<DatasetProcessStatus> statusResults = null;
    PagingRequest statusPage = new PagingRequest(0, PAGING_LIMIT);
    do {
      try {
        statusResults = statusService.listDatasetProcessStatus(d.getKey(), statusPage);

        List<DatasetProcessStatus> results = statusResults.getResults();
        // check if latest crawl history has empty start date indicative of phantom crawl and potential false positive!
        if (results != null && !results.isEmpty() && results.get(0) != null && results.get(0).getStartedCrawling() == null) {
          throw new RuntimeException("PFP on " + d.getKey() + " " + results.get(0));
        }

        // datasets that haven't been crawled yet, and registered before cutoff date are potential orphans
        if (statusResults.getCount() == 0) {
          LOG.warn("Dataset never crawled");
        } else {
          for (DatasetProcessStatus st : results) {
            LOG.debug("{} processing {}", d.getKey(), st);
            FinishReason finishReason = st.getFinishReason(); // NORMAL, USER_ABORT, ABORT, NOT_MODIFIED, UNKNOWN
            Date finishedCrawling = st.getFinishedCrawling();
            if (finishReason == NORMAL) {
              if (st.getCrawlJob().getEndpointType() != EndpointType.DWC_ARCHIVE) {
                throw new RuntimeException("Most recent wasn't a DWCA");
              }

              int attempt = st.getCrawlJob().getAttempt();

              ProcessBuilder processBuilder = new ProcessBuilder();
              processBuilder.redirectOutput(new File("/dev/null"));
              processBuilder.command("scp", "-p", "crap@prodcrawler1-vh.gbif.org:storage/dwca/"+datasetKey+"/"+datasetKey+"."+attempt+".dwca", datasetKey+".dwca");
              try {
                Process process = processBuilder.start();
                int exitVal = process.waitFor();
                if (exitVal == 0) {
                  LOG.info("Success with {} archive!", attempt);
                  return;
                } else {
                  processBuilder.command("scp", "-p", "crap@prodcrawler1-vh.gbif.org:storage/dwca/"+datasetKey+"/"+datasetKey+".0.dwca", datasetKey+".dwca");
                  process = processBuilder.start();
                  exitVal = process.waitFor();
                  if (exitVal == 0) {
                    LOG.info("Success with 0 archive!");
                    return;
                  }
                }

                LOG.error("Problem with "+datasetKey);
              } catch (IOException e) {
                e.printStackTrace();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        }
      } catch (Exception exception) {
        LOG.warn("Failure on dataset " + d.getKey(), exception);
      }
      statusPage.nextPage();
    } while (statusResults != null && !statusResults.isEndOfRecords());

  }

  /**
   * Retrieves a dataset from the crawler cache (not really), and either updates the registry or generates an IPT resource folder.
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
    cacheRescue.setName("crawlerDwcaCacheTime");
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
    Endpoint oldEndpoint = dataset.getEndpoints().stream()
      .filter(ep -> ep.getType() == EndpointType.DWC_ARCHIVE)
      .findFirst()
      .get();

    if (!oldEndpoint.getUrl().toString().contains("orphans.gbif.org")) {
      datasetService.deleteEndpoint(uuid, oldEndpoint.getKey());

      String endPointDirectory = null;

      if (node.getType() == NodeType.COUNTRY) {
        endPointDirectory = organization.getCountry().getIso2LetterCode().toUpperCase();
      } else {
        endPointDirectory = organization.getEndorsingNodeKey().toString();
      }

      Endpoint newEndpoint = new Endpoint();
      newEndpoint.setType(EndpointType.DWC_ARCHIVE);
      newEndpoint.setUrl(new URI("https://orphans.gbif.org/" + endPointDirectory + "/" + datasetKey + ".zip"));
      newEndpoint.setDescription("Orphaned dataset awaiting adoption.");
      datasetService.addEndpoint(uuid, newEndpoint);
    }
  }

  public static void main(String... args)
    throws ParseException, IOException, ParserConfigurationException, SAXException, TemplateException,
    NoSuchFieldException, InterruptedException, URISyntaxException {
    WatchdogModule watchdogModule = new WatchdogModule();

    DatasetRescuerFromDwcaCache rescuer = new DatasetRescuerFromDwcaCache(watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(), watchdogModule.setupNodeService(), watchdogModule.setupDatasetProcessStatusService());

    if (args.length < 1) {
      System.err.println("Give dataset keys as argument");
      System.exit(1);
    }

    List<String> datasets = Files.readAllLines(Paths.get("gone-offline-but-have-dwca"));

    for (String datasetKey : datasets) {
      LOG.info("Rescuing {}", datasetKey);
//      rescuer.rescue(datasetKey);
      rescuer.findFinalCachedArchive(datasetKey);
    }
  }

}
