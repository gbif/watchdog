package org.gbif;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.gbif.api.model.Constants;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Program scans all datasets registered in GBIF for potential orphaned datasets.
 */
public class OrphanDatasetScanner {
  public static final Instant CUTOFF = Instant.parse("2025-03-01T00:00:00Z");
  public static final Date D_CUTOFF = Date.from(CUTOFF);

  private static Logger LOG = LoggerFactory.getLogger(OrphanDatasetScanner.class);

  private static final int PAGING_LIMIT = 100;

  private final DatasetService datasetService;
  private final OrganizationService organizationService;
  private final DatasetProcessStatusService statusService;
  private final CrawlChecker lastUsefulCrawl;

  private Map<String, List<String[]>> orphansByParticipant;
  private List<String[]> orphansRescued;
  private List<String[]> nonOrphansJustInvalid;
  private final File outputDirectory;

  OrphanDatasetScanner(DatasetService datasetService, OrganizationService organizationService, DatasetProcessStatusService statusService) throws IOException {
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.statusService = statusService;
    this.lastUsefulCrawl = new CrawlChecker(statusService);
    orphansByParticipant = Maps.newHashMap();
    orphansRescued = Lists.newArrayList();
    nonOrphansJustInvalid = Lists.newArrayList();
    outputDirectory = org.gbif.utils.file.FileUtils.createTempDir();
    LOG.info("Output directory is {}", outputDirectory);
  }

  /**
   * Iterates over all datasets registered with GBIF checking for orphaned datasets. A dataset is flagged as a potential
   * orphan if it hasn't been crawled successfully within the last X months.
   */
  public void scan() {
    PagingRequest datasetPage = new PagingRequest(0, PAGING_LIMIT);
    int datasets = 0;

    Map<UUID, Organization> organizationCache = new HashMap<>();

    // iterate through all datasets
    PagingResponse<Dataset> datasetResults;
    do {
      datasetResults = datasetService.list(datasetPage);
      for (Dataset d : datasetResults.getResults()) {
        if (datasets % PAGING_LIMIT == 0) {
          LOG.info("Iterated over " + datasets + " datasets");
        }
        datasets++;
        if (d.getPublishingOrganizationKey().equals(Constants.PLAZI_ORG_KEY)) {
          continue;
        }
        LOG.debug("{} ({})", d.getKey(), datasets);

        Organization organization = organizationCache.get(d.getPublishingOrganizationKey());
        if (organization == null) {
          organization = organizationService.get(d.getPublishingOrganizationKey());
          organizationCache.put(d.getPublishingOrganizationKey(), organization);
        }

        if (!toIgnore(d, organization)) {
          Pair<Date,Integer> lastGoodCrawl = lastUsefulCrawl.lastUsefulCrawl(d);

          // check: was the most recent crawl successful, and did it occur before cutoff?
          if (lastGoodCrawl == null) {
            LOG.warn("\t{}   IS    UNBORN , never crawled", d.getKey());
          } else if (lastGoodCrawl.getLeft().after(D_CUTOFF)) {
            LOG.debug("\t{} is not orphaned, crawl {} on {}", d.getKey(), lastGoodCrawl.getRight(), lastGoodCrawl.getLeft());
          } else {
            LOG.info("\t{}   IS   orphaned, crawl {} on {}", d.getKey(), lastGoodCrawl.getRight(), lastGoodCrawl.getLeft());
          }
        }
      }
       datasetPage.nextPage();
    } while (!datasetResults.isEndOfRecords());
    LOG.info("Finished after checking {} datasets", datasets);
  }


  /**
   * Checks if dataset should be ignored for consideration as an orphan.
   *
   * @param dataset dataset
   * @param organization organization
   *
   * @return true if dataset should be skipped, false otherwise
   */
  private boolean toIgnore(Dataset dataset, Organization organization) {
    // ignore metadata-only, as there's no point rescuing them
    if (dataset.getType() == DatasetType.METADATA) {
      return true;
    }

    // ignore the Catalogue of Life
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("f4ce3c03-7b38-445e-86e6-5f6b04b649d4"))) {
      return true;
    }
    // ignore Plazi - more than 1600 false positives
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("ffb07bec-2d10-492d-9c37-361fe0b79427"))) {
      return true;
    }
    return false;
  }

  public static void main(String[] args) throws ParseException, IOException {
    WatchdogModule watchdogModule = new WatchdogModule();

    OrphanDatasetScanner scanner = new OrphanDatasetScanner(watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(), watchdogModule.setupDatasetProcessStatusService());

    scanner.scan();
  }
}
