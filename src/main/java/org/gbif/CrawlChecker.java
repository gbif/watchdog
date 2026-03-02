package org.gbif;

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetProcessStatusService;
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
import static org.gbif.api.vocabulary.EndpointType.DWC_ARCHIVE;
import static org.gbif.api.vocabulary.EndpointType.EML;

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
            LOG.debug("\t{} processing {}", d.getKey(), st);
            LOG.info("\t{} {} status {} time {}", d.getKey(), st.getCrawlJob().getAttempt(), st.getFinishReason(), st.getFinishedCrawling());
            FinishReason finishReason = st.getFinishReason(); // NORMAL, USER_ABORT, ABORT, NOT_MODIFIED, UNKNOWN

            if (finishReason == null || st.getFinishedCrawling() == null) {
              LOG.info("\t{} is currently crawling (attempt {})", d.getKey(), st.getCrawlJob().getAttempt());
              // Current crawl
              continue;
            }

            if (mostRecentCrawlStatus == null) {
              mostRecentCrawlStatus = finishReason.toString();
            }

            if (finishReason == NOT_MODIFIED) {
              LOG.debug("\t{} is not modified, crawl {} on {}", d.getKey(), st.getCrawlJob().getAttempt(), st.getFinishedCrawling());

              // Record the hash of the not-modified file, and check it matches with the hash of the earlier normal file.
              // Avoids the case where the not-modified file is an error page or similar.

              String hash = dwcaHashFromCache(d.getKey().toString(), st.getCrawlJob().getAttempt());
              LOG.info("\tHashed {} {} to {}", d.getKey(), st.getCrawlJob().getAttempt(), hash);

              if (notModifiedDate == null || !notModifiedHash.equals(hash)) {
                notModifiedDate = st.getFinishedCrawling();
                notModifiedHash = hash;
              }
              continue;
            }

            if (finishReason == NORMAL) {
              // Use most recent not-modified date if there are more recent crawls with the same data
              String hash;
              if (st.getCrawlJob().getEndpointType() == DWC_ARCHIVE || st.getCrawlJob().getEndpointType() == EML) {
                hash = dwcaHashFromCache(d.getKey().toString(), st.getCrawlJob().getAttempt());
                LOG.info("\tHashed {} {} to {}", d.getKey(), st.getCrawlJob().getAttempt(), hash);
              } else {
                hash = "Not_DWCA_or_EML";
              }
              if (notModifiedDate != null && !notModifiedHash.equals(hash)) {
                // Hashes don't match, ignore the subsequent not-modified crawl (this must be normal, abort, not_modified).
                notModifiedDate = st.getFinishedCrawling();
                notModifiedHash = hash;
              }

              Date finishedCrawling = (notModifiedDate == null) ? st.getFinishedCrawling() : notModifiedDate;
              return Pair.of(finishedCrawling, st.getCrawlJob().getAttempt());
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

  private String dwcaHashFromCache(String datasetKey, int attempt) {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command("ssh", "crap@prodcrawler1-vh.gbif.org", "md5sum", "storage/dwca/"+datasetKey+"/"+datasetKey+"."+attempt+".dwca");
    try {
      Process process = processBuilder.start();

      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = reader.readLine();
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
