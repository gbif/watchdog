package org.gbif;

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.vocabulary.EndpointType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;

import static org.gbif.OrphanDatasetScanner.D_CUTOFF;
import static org.gbif.api.model.crawler.FinishReason.NORMAL;
import static org.gbif.api.model.crawler.FinishReason.NOT_MODIFIED;

public class CrawlChecker {
  private static Logger LOG = LoggerFactory.getLogger(CrawlChecker.class);
  private static final int PAGING_LIMIT = 100;

  private final DatasetProcessStatusService statusService;

  public CrawlChecker(DatasetProcessStatusService statusService) throws IOException {
    this.statusService = statusService;
  }

  public Pair<Date, Integer> lastUsefulCrawl(Dataset d) {
    String mostRecentCrawlStatus = null;
    Date notModifiedDate = null;
    String notModifiedHash = null;

    // iterate through the dataset's crawl history
    PagingResponse<DatasetProcessStatus> statusResults = null;
    PagingRequest statusPage = new PagingRequest(0, PAGING_LIMIT);
    do {
      try {
        statusResults = statusService.listDatasetProcessStatus(d.getKey(), statusPage);

        List<DatasetProcessStatus> results = statusResults.getResults();

        // datasets that haven't been crawled yet, but were registered before the cutoff date are potential orphans
        if (statusResults.getCount() == 0 && d.getCreated().before(D_CUTOFF)) {
          return Pair.of(null, -1);
        } else {
          for (DatasetProcessStatus st : results) {
            String datasetKey = d.getKey().toString();
            int attempt = st.getCrawlJob().getAttempt();
            EndpointType endpointType = st.getCrawlJob().getEndpointType();
            LOG.debug("\t{} processing {}", datasetKey, st);
            LOG.info("\t{} {} status {} time {}", datasetKey, attempt, st.getFinishReason(), st.getFinishedCrawling());
            FinishReason finishReason = st.getFinishReason(); // NORMAL, USER_ABORT, ABORT, NOT_MODIFIED, UNKNOWN

            if (finishReason == null || st.getFinishedCrawling() == null) {
              LOG.info("\t{} is currently crawling (attempt {})", datasetKey, attempt);
              // Current crawl
              continue;
            }

            if (mostRecentCrawlStatus == null) {
              mostRecentCrawlStatus = finishReason.toString();
            }

            if (finishReason == NOT_MODIFIED) {
              LOG.debug("\t{} is not modified, crawl {} on {}", datasetKey, attempt, st.getFinishedCrawling());

              // Record the hash of the not-modified file, and check it matches with the hash of the earlier normal file.
              // Avoids the case where the not-modified file is an error page or similar.

              String hash = hashFromCache(datasetKey, attempt, endpointType);
              LOG.info("\tHashed {} {} {} to {}", datasetKey, attempt, endpointType, hash);
              if (hash == null) {
                continue;
              }

              if (notModifiedDate == null || !notModifiedHash.equals(hash)) {
                notModifiedDate = st.getFinishedCrawling();
                notModifiedHash = hash;
              }
              continue;
            }

            if (finishReason == NORMAL) {
              // Use most recent not-modified date if there are more recent crawls with the same data
              if (notModifiedDate != null) {
                String hash = hashFromCache(datasetKey, attempt, endpointType);
                LOG.info("\tHashed {} {} {} to {}", datasetKey, attempt, endpointType, hash);

                if (!notModifiedHash.equals(hash)) {
                  // Hashes don't match, ignore the subsequent not-modified crawl (this must be normal, abort, not_modified).
                  notModifiedDate = st.getFinishedCrawling();
                  notModifiedHash = hash;
                }
              }

              Date finishedCrawling = (notModifiedDate == null) ? st.getFinishedCrawling() : notModifiedDate;
              return Pair.of(finishedCrawling, attempt);
            }
          }
        }
      } catch (Exception exception) {
        LOG.warn("\tFailure on dataset " + d.getKey(), exception);
      }
      statusPage.nextPage();
    } while (statusResults != null && !statusResults.isEndOfRecords());

    return null;
  }

  private String hashFromCache(String datasetKey, int attempt, EndpointType endpointType) {
    final String path;
    switch (endpointType) {
      case CAMTRAP_DP:
        path = "storage/camtrapdp/"+datasetKey+"/"+datasetKey+"."+attempt+".camtrapdp";
        break;
      case DWC_ARCHIVE:
      case EML:
        path = "storage/dwca/"+datasetKey+"/"+datasetKey+"."+attempt+".dwca";
        break;
      case DIGIR:
      case TAPIR:
      case BIOCASE:
      case DIGIR_MANIS:
        path = "storage/xml/"+datasetKey+"/"+attempt+".tar.xz";
        break;

      case BIOCASE_XML_ARCHIVE:
        path = "storage/abcda/"+datasetKey+"."+attempt+".abcda";
        break;

      default:
        LOG.error("\t{}: Unknown endpoint type {}", datasetKey, endpointType);
        return null;
    }

    ProcessBuilder processBuilder = new ProcessBuilder();
    //LOG.info("\t{}: Hashing {}", datasetKey, path);
    processBuilder.command("ssh", "crap@prodcrawler1-vh.gbif.org", "md5sum", path);
    try {
      Process process = processBuilder.start();

      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = reader.readLine();
      if (line == null) {
        return null;
      }
      String hash = line.split(" ")[0];

      int exitVal = process.waitFor();
      if (exitVal == 0) {
        return hash;
      } else {
        LOG.error("Problem finding hash for {} {}", datasetKey, attempt);
        return null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }
}
