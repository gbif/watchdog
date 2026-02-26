package org.gbif;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.Constants;
import org.gbif.api.model.checklistbank.DatasetMetrics;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Node;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.checklistbank.DatasetMetricsService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.common.parsers.CountryParser;
import org.gbif.watchdog.config.WatchdogModule;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.beust.jcommander.internal.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.checklistbank.DatasetMetrics;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Node;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.checklistbank.DatasetMetricsService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.*;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.common.parsers.CountryParser;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

import static org.gbif.api.model.crawler.FinishReason.NORMAL;
import static org.gbif.api.model.crawler.FinishReason.NOT_MODIFIED;
import static org.gbif.api.vocabulary.EndpointType.DWC_ARCHIVE;

/**
 * Program scans all datasets registered in GBIF for potential orphaned datasets.
 */
public class OrphanDatasetScanner {
  private static Logger LOG = LoggerFactory.getLogger(OrphanDatasetScanner.class);
  private static final Date CUTOFF_DATE = Date.from(Instant.parse("2025-03-01T00:00:00Z"));
  private static final SimpleDateFormat ISO_8601_SDF = new SimpleDateFormat("yyyy-MM-dd");
  private static final Pattern escapeChars = Pattern.compile("[\t\n\r]");
  private static final String TSV_EXTENSION = ".tsv";
  private static final String YES = "YES";
  private static final String PNMC = "Participant Node Managers Committee";
  private static final String FILENAME_INVALID = "onlineButInvalid";
  private static final String FILENAME = "toRescueIn2022";
  private static final int PAGING_LIMIT = 100;
  // timeout in milliseconds for both the connection timeout and the response read timeout
  private static final int TIMEOUT_MILLIS = 500;

  private final DatasetService datasetService;
  private final OrganizationService organizationService;
  private final NodeService nodeService;
  private final InstallationService installationService;
  private final DatasetMetricsService datasetMetricsService;
  private final OccurrenceSearchService occurrenceSearchService;
  private final DatasetProcessStatusService statusService;

  private Map<String, List<String[]>> orphansByParticipant;
  private List<String[]> orphansRescued;
  private List<String[]> nonOrphansJustInvalid;
  private final File outputDirectory;

  OrphanDatasetScanner(DatasetService datasetService, OrganizationService organizationService, NodeService nodeService,
    InstallationService installationService,
    DatasetMetricsService datasetMetricsService, OccurrenceSearchService occurrenceSearchService,
    DatasetProcessStatusService statusService) throws IOException {
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;
    this.installationService = installationService;
    this.datasetMetricsService = datasetMetricsService;
    this.occurrenceSearchService = occurrenceSearchService;
    this.statusService = statusService;
    orphansByParticipant = Maps.newHashMap();
    orphansRescued = Lists.newArrayList();
    nonOrphansJustInvalid = Lists.newArrayList();
    outputDirectory = org.gbif.utils.file.FileUtils.createTempDir();
  }

  /**
   * Iterates over all datasets registered with GBIF checking for orphaned datasets. A dataset is flagged as a potential
   * orphan if it hasn't been crawled successfully within the last X months.
   */
  public void scan() {
    PagingRequest datasetPage = new PagingRequest(0, PAGING_LIMIT);
    int datasets = 0;
    int orphans = 0;

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
          boolean orphaned = true;
          String mostRecentCrawlEndpointType = null;
          String mostRecentCrawlEndpointUri = null;
          String mostRecentCrawlStatus = null;
          String mostRecentCrawlDate = null;
          boolean potentialFalsePositive = false;
          Date notModifiedDate = null;
          String notModifiedHash = null;

          // iterate through the dataset's crawl history
          PagingResponse<DatasetProcessStatus> statusResults = null;
          PagingRequest statusPage = new PagingRequest(0, PAGING_LIMIT);
          statusLoop:
          do {
            try {
              statusResults = statusService.listDatasetProcessStatus(d.getKey(), statusPage);

              List<DatasetProcessStatus> results = statusResults.getResults();

              // datasets that haven't been crawled yet, but were registered before the cutoff date are potential orphans
              if (statusResults.getCount()==0 && beforeCutoff(d.getCreated())) {
                orphaned = true;
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
                    mostRecentCrawlDate = convertToIsoDate(st.getFinishedCrawling());
                    mostRecentCrawlEndpointType = (st.getCrawlJob().getEndpointType() == null) ? "" : st.getCrawlJob().getEndpointType().toString();
                    mostRecentCrawlEndpointUri = (st.getCrawlJob().getTargetUrl() == null) ? "" : st.getCrawlJob().getTargetUrl().toString();
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
                    if (st.getCrawlJob().getEndpointType() == DWC_ARCHIVE) {
                      hash = dwcaHashFromCache(d.getKey().toString(), st.getCrawlJob().getAttempt());
                    } else {
                      hash = "notdwca";
                    }
                    if (notModifiedDate != null && !notModifiedHash.equals(hash)) {
                      // Hashes don't match, ignore the subsequent not-modified crawl (this must be normal, abort, not_modified).
                      notModifiedDate = st.getFinishedCrawling();
                      notModifiedHash = hash;
                    }
                    Date finishedCrawling = (notModifiedDate == null) ? st.getFinishedCrawling() : notModifiedDate;

                    // check: was the most recent crawl successful, and did it occur before cutoff?
                    if (afterCutoff(finishedCrawling)) {
                      orphaned = false;
                      LOG.debug("\t{} is not orphaned, crawl {} on {}", d.getKey(), st.getCrawlJob().getAttempt(), finishedCrawling);
                      break statusLoop;
                    }
                    // otherwise check: was this a successful crawl that occurred before cutoff?
                    if (beforeCutoff(finishedCrawling)) {
                      LOG.info("\t{}   IS   orphaned, crawl {} on {}", d.getKey(), st.getCrawlJob().getAttempt(), finishedCrawling);
                      orphaned = true;
                      break statusLoop;
                    }
                  }
                }
              }
            } catch (Exception exception) {
              LOG.warn("\tFailure on dataset " + d.getKey(), exception);
            }
            statusPage.nextPage();
          } while (statusResults != null && !statusResults.isEndOfRecords());

          // Warning: excluding false positives due to phantom crawl issue until it gets fixed: https://github.com/gbif/crawler/issues/3
          if (orphaned) {
            orphans++;
            Node node = nodeService.get(organization.getEndorsingNodeKey());
            String[] record =
              getRecord(d, organization, mostRecentCrawlEndpointType, mostRecentCrawlEndpointUri, mostRecentCrawlStatus,
                mostRecentCrawlDate);

            // Warning: excluding false positives that are online but not indexed because they are invalid! Note that fixing https://github.com/gbif/crawler/issues/9 would help discover these.
            boolean online = Boolean.valueOf(record[19]); // corresponds to column "online?"
            int numOccurrences = Integer.valueOf(record[4]);
            int numUsages = Integer.valueOf(record[5]);
            if (online) {
              nonOrphansJustInvalid.add(record);
            }
            else {
              String key = (node.getParticipantTitle() == null) ? PNMC : node.getParticipantTitle();
              orphansByParticipant.computeIfAbsent(key, v -> Lists.newArrayList()).add(record);
              orphansRescued.add(record.clone());
            }
          }
        }
      } datasetPage.nextPage();
    } while (!datasetResults.isEndOfRecords());
    LOG.info("Total # of datasets: " + datasets);
    LOG.info("Total # of orphaned datasets: " + orphans);

    // write false positives that are online but invalid to file, to facilitate Jan following up on them
    writeListToFiles(FILENAME_INVALID, nonOrphansJustInvalid);
    // write orphans to file, separated by participant
    writeMapToFiles(orphansByParticipant);
    // write all orphans to be rescued in 2022 to file
    writeListToFiles(FILENAME, orphansRescued);
  }

  /**
   * For each entry in Map<String, List<String>>, method writes list of strings to new file having the name of the key.
   * The header and each string in the list is a tab row whose columns correspond to each other.
   */
  private void writeMapToFiles(Map<String, List<String[]>> map) {
    LOG.info("Writing orphans to file - one for each participant..");
    // sort map by keys
    ImmutableSortedMap<String, List<String[]>> sortedMap =
      ImmutableSortedMap.copyOf(map, Ordering.natural().nullsFirst());

    int files = 0;
    for (String p : sortedMap.keySet()) {
      files++;
      Writer writer;
      File out = null;
      try {
        String fileName = p + TSV_EXTENSION;
        fileName = fileName.replaceAll(" ", "");
        out = new File(outputDirectory, fileName);
        writer = org.gbif.utils.file.FileUtils.startNewUtf8File(out);

        // write header to output file
        writer.write(getHeader());

        // write records to output file
        int datasets = 0;
        Set<String> installations = Sets.newHashSet();
        long numOccurrences = 0;
        String country = null;
        for (String[] r : sortedMap.get(p)) {
          writer.write(tabRow(r));
          datasets++;
          installations.add(r[6]);
          numOccurrences = numOccurrences + Integer.valueOf(r[4].replaceAll("\"", ""));
          country = r[11].replaceAll("\"", "");
        }
        writer.close();

        long countryOccurrenceCount = 0;
        if (country != null && country.equalsIgnoreCase(p)) {
          countryOccurrenceCount = countryCount(CountryParser.getInstance().parse(country).getPayload());
        }

        // % occurrences orphaned?
        int percentageOccurrencesOrphaned =
          (countryOccurrenceCount > 0) ? Math.round((numOccurrences * 100 / countryOccurrenceCount)) : 0;

        // logging below used to generate GitHub markdown table
        System.out.println("| " + p + " | " + datasets + " | " + installations.size() + " | " + numOccurrences + " | "
                           + countryOccurrenceCount + " | " + percentageOccurrencesOrphaned + " | "
                           + "[View](https://github.com/gbif/watchdog/blob/master/lists/orphans202007/" + fileName
                           + ") / [Download](https://raw.githubusercontent.com/gbif/watchdog/master/lists/orphans202007/"
                           + fileName + ") |");
      } catch (IOException e) {
        LOG.error("Exception while writing to output file: " + out.getAbsolutePath());
      }
    }
    LOG.info("Total number of files written: " + files);
    LOG.info("Files written to: " + outputDirectory.getAbsolutePath());
  }

  /**
   * For each entry in List<String[]>, method writes array of strings to new file.
   * The header and each string in the list is a tab row whose columns correspond to each other.
   *
   * @param name name of file to write to
   */
  private void writeListToFiles(@NotNull String name, List<String[]> ls) {
    LOG.info("Writing all orphans to file..");
    Writer writer;
    File out = null;
    try {
      String fileName =  name + TSV_EXTENSION;
      out = new File(outputDirectory, fileName);
      writer = org.gbif.utils.file.FileUtils.startNewUtf8File(out);

      // write header to output file
      writer.write(getHeader());

      // write records to output file
      for (String[] r : ls) {
        writer.write(tabRow(r));
      }
      writer.close();
    } catch (IOException e) {
      LOG.error("Exception while writing to output file: " + out.getAbsolutePath());
    }
    LOG.info("All orphans written to: " + out.getAbsolutePath());
  }

  /**
   * @param date date
   * @return date in ISO 8601, e.g. to facilitate sorting
   */
  @NotNull
  private String convertToIsoDate(@NotNull Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return ISO_8601_SDF.format(cal.getTime());
  }

  /**
   * @return header as tab row
   */
  @NotNull
  private String getHeader() {
    String[] header =
      new String[] {"datasetTitle", "datasetKey", "datasetType", "hasEndpoints", "numOccurrences", "numNameUsages",
        "installationKey", "installationType", "organisationKey", "organisationTitle", "participantTitle",
        "countryOfParticipant", "gbifRegistrationDate", "mostRecentCrawlEndpointType", "mostRecentCrawlEndpointUri",
        "mostRecentCrawlDate", "mostRecentCrawlStatus", "potentialFalsePositive?","orphaned?",
        "numImages", "online?"};
    return tabRow(header);
  }

  /**
   * @param dataset                     dataset
   * @param organization                owning organisation of dataset
   * @param mostRecentCrawlEndpointType EndpointType of most recent crawled Endpoint
   * @param mostRecentCrawlEndpointUri  URI of most recent crawled Endpoint
   * @param mostRecentCrawlStatus       Status of last crawl, either NORMAL, USER_ABORT, ABORT, NOT_MODIFIED, UNKNOWN
   * @param mostRecentCrawlDate         Date of most recent crawl in ISO 8601 format
   *
   * @return record as String array
   */
  @NotNull
  private String[] getRecord(Dataset dataset, Organization organization, String mostRecentCrawlEndpointType,
    String mostRecentCrawlEndpointUri, String mostRecentCrawlStatus, String mostRecentCrawlDate) {

    Installation installation = installationService.get(dataset.getInstallationKey());
    Node node = nodeService.get(organization.getEndorsingNodeKey());
    Country country = (node.getCountry() == null) ? Country.UNKNOWN : node.getCountry();

    // date dataset was registered in ISO 8601, to facilitate sorting
    String registered = convertToIsoDate(dataset.getCreated());

    // does the dataset have any endpoints?
    boolean hasEndpoints = dataset.getEndpoints().size() > 0;

    // how many occurrence records?
    OccurrenceSearchRequest countRequest = new OccurrenceSearchRequest();
    countRequest.setLimit(0);
    countRequest.addDatasetKeyFilter(dataset.getKey());
    long numOccurrences = occurrenceSearchService.search(countRequest).getCount();

    // how many name usages?
    long numNameUsages = 0;
    DatasetMetrics metrics = datasetMetricsService.get(dataset.getKey());
    if (metrics != null) {
      numNameUsages = metrics.getUsagesCount();
    }

    // how many images?
    long imageCount = imageCount(dataset.getKey());

    // URL responds successfully?
    boolean online = false;
    if (mostRecentCrawlEndpointUri != null) {
      online = pingURL(mostRecentCrawlEndpointUri);
    }

    return new String[] {dataset.getTitle(), dataset.getKey().toString(), dataset.getType().toString(),
      String.valueOf(hasEndpoints), String.valueOf(numOccurrences), String.valueOf(numNameUsages),
      installation.getKey().toString(), installation.getType().toString(), organization.getKey().toString(),
      organization.getTitle(), node.getParticipantTitle(), country.getTitle(), registered, mostRecentCrawlEndpointType,
      mostRecentCrawlEndpointUri, mostRecentCrawlDate, mostRecentCrawlStatus, YES,
      String.valueOf(imageCount), String.valueOf(online).toUpperCase()};
  }

  /**
   * Check if date occurred before cutoff date.
   *
   * @param date cutoff date
   * @return true if date happened before cutoff date, false otherwise
   */
  boolean beforeCutoff(@NotNull Date date) throws ParseException {
    return CUTOFF_DATE.compareTo(date) > 0;
  }

  /**
   * Check if date occurred after the cutoff date.
   *
   * @param date cutoff date
   * @return true if date happened on or after the cutoff date, false otherwise
   */
  private boolean afterCutoff(@NotNull Date date) throws ParseException {
    return !beforeCutoff(date);
  }

 /*
 * Return the number of occurrences published by a dataset.
 */
  private long countryCount(Country country) {
    SearchResponse response = null;
    try {
      OccurrenceSearchRequest req = new OccurrenceSearchRequest();
      req.addParameter(OccurrenceSearchParameter.PUBLISHING_COUNTRY, country.getIso2LetterCode());
      req.setLimit(1);
      response = occurrenceSearchService.search(req);
    } catch (ServiceUnavailableException e) {
      LOG.error("Unable to retrieve country count", e);
    }
    return (response != null && response.getCount() != null) ? response.getCount() : 0;
  }

  /*
   * Return the number of images associated to a dataset.
   */
  private long imageCount(@NotNull UUID datasetKey) {
    SearchResponse response = null;
    try {
      OccurrenceSearchRequest req = new OccurrenceSearchRequest();
      req.addDatasetKeyFilter(datasetKey);
      req.addParameter(OccurrenceSearchParameter.MEDIA_TYPE, MediaType.StillImage);
      req.setLimit(1);
      response = occurrenceSearchService.search(req);
    } catch (ServiceUnavailableException e) {
      LOG.error("Unable to retrieve image count for dataset" + datasetKey.toString(), e);
    }
    return (response != null && response.getCount() != null) ? response.getCount() : 0;
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
    // ignore the Catalogue of Life
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("f4ce3c03-7b38-445e-86e6-5f6b04b649d4"))) {
      return true;
    }
    // ignore Plazi - more than 1600 false positives
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("ffb07bec-2d10-492d-9c37-361fe0b79427"))) {
      return true;
    }
    // ignore OBIS
    // if (organization.getEndorsingNodeKey().equals(UUID.fromString("ba0670b9-4186-41e6-8e70-f9cb3065551a"))) {
    //   return true;
    // }
    // eBird, because it gets published once a year without fail
     if (dataset.getKey().equals(UUID.fromString("4fa7b334-ce0d-4e88-aaae-2e0c138d049e"))) {
       return true;
     }
    return false;
  }

  /**
   * Method derived from https://stackoverflow.com/a/3584332 and tested in OrphanDatasetScannerTest.
   * Pings an HTTP URL with HEAD request and returns <code>true</code> if the response code is in 200-399 range.
   *
   * @param url     The HTTP URL to be pinged
   *
   * @return <code>true</code> if the given HTTP URL has returned response code 200-399 on a HEAD request within the
   * given timeout, <code>false</code> otherwise
   */
  @VisibleForTesting
  public static boolean pingURL(String url) {
    // Endpoints not actually online, but nevertheless give a successful response:
    // http://bijh.zrc-sazu.si/DIGIR/digir.php Digir Installation from Slovenia 5ff785c2-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://bijh.zrc-sazu.si/DIGIR/digir.php")) {
      return false;
    }
    // http://w2.scarmarbin.be/digir2/digir.php DiGIR installation from ANTABIF 601e20f6-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://w2.scarmarbin.be/digir2/digir.php")) {
      return false;
    }
    // http://www.ots.ac.cr/herbarium/gbif/dwca-herbariumlc.zip HTTP installation from Costa Rica 9976bbce-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://www.ots.ac.cr/herbarium/gbif/dwca-herbariumlc.zip")) {
      return false;
    }
    // http://acoi.ci.uc.pt/digir/www/DiGIR.php DiGIR installation from Portugal 5ff54d98-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://acoi.ci.uc.pt/digir/www/DiGIR.php")) {
      return false;
    }
    // http://www.gbif.org.nz/tapirlink/tapir.php/NZBRN TAPIR installation from New Zealand that redirects to GBIF.org
    if (url.equalsIgnoreCase("http://www.gbif.org.nz/tapirlink/tapir.php/NZBRN")) {
      return false;
    }

    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setConnectTimeout(TIMEOUT_MILLIS);
      connection.setReadTimeout(TIMEOUT_MILLIS);
      connection.setRequestMethod("HEAD");
      connection.setInstanceFollowRedirects(true);
      int responseCode = connection.getResponseCode();
      connection.disconnect();
      return (200 <= responseCode && responseCode <= 399);
    } catch (IOException exception) {
      return false;
    }
  }

  /**
   * Generate a row/string of values tab delimited. Line breaking characters encountered in
   * a value are replaced with an empty character.
   *
   * @param columns array of values/columns
   *
   * @return row/string of values tab delimited
   */
  @NotNull
  public static String tabRow(String[] columns) {
    // escape \t \n \r chars, and wrap in double quotes
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] != null) {
        columns[i] = "\"" + StringUtils.trimToNull(escapeChars.matcher(columns[i]).replaceAll(" ")) + "\"";
      }
    }
    return StringUtils.join(columns, '\t') + "\n";
  }

  /**
   * Iterates over all datasets registered with GBIF checking if their first crawl history is a phantom crawl,
   * meaning it has no start date.
   */
  public void scanPhantoms() {
    PagingRequest datasetPage = new PagingRequest(0, PAGING_LIMIT);
    int datasets = 0;
    int phantoms = 0;
    // iterate through all datasets
    PagingResponse<Dataset> datasetResults;
    do {
      datasetResults = datasetService.list(datasetPage);
      //datasetService.list(new DatasetRequestSearchParams());
      for (Dataset d : datasetResults.getResults()) {
        datasets++;
        // iterate through the dataset's crawl history
        PagingResponse<DatasetProcessStatus> statusResults = null;
        PagingRequest statusPage = new PagingRequest(0, PAGING_LIMIT);
        do {
          try {
            statusResults = statusService.listDatasetProcessStatus(d.getKey(), statusPage);
            List<DatasetProcessStatus> results = statusResults.getResults();
            // check latest crawl history has empty start date (indicative of phantom crawl)
            boolean phantom = (results != null && !results.isEmpty() && results.get(0) != null && results.get(0).getStartedCrawling() == null);

            if (phantom) {
              phantoms++;
              LOG.error("Phantom crawl for dataset: " + d.getKey());
            }
          } catch (Exception exception) {
            LOG.error("Failure iterating: Dataset " + d.getKey() + " Exception: " + exception.getMessage());
          }
          statusPage.nextPage();
        } while (statusResults != null && !statusResults.isEndOfRecords());
      } datasetPage.nextPage();
    } while (!datasetResults.isEndOfRecords());
    LOG.info("Total # of datasets: " + datasets);
    LOG.info("Total # of phantom crawls: " + phantoms);
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

  public static void main(String[] args) throws ParseException, IOException {
    WatchdogModule watchdogModule = new WatchdogModule();
    DatasetRescuerFromDwcaCache rescuer = new DatasetRescuerFromDwcaCache(watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(), watchdogModule.setupNodeService(), watchdogModule.setupDatasetProcessStatusService());

    OrphanDatasetScanner scanner = new OrphanDatasetScanner(watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(), watchdogModule.setupNodeService(),
      watchdogModule.setupInstallationService(),
      watchdogModule.setupDatasetMetricsService(), watchdogModule.setupOccurrenceSearchService(),
      watchdogModule.setupDatasetProcessStatusService());
    scanner.scan();
  }
}
