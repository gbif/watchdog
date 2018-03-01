package org.gbif;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.checklistbank.DatasetMetrics;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Node;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.checklistbank.DatasetMetricsService;
import org.gbif.api.service.metrics.CubeService;
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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.beust.jcommander.internal.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Program scans all datasets registered in GBIF for potential orphaned datasets.
 */
public class OrphanDatasetScanner {
  private static Logger LOG = LoggerFactory.getLogger(OrphanDatasetScanner.class);
  private static final String CUTOFF_DATE = "2017-04-25";
  private static final SimpleDateFormat ISO_8601_SDF = new SimpleDateFormat("yyyy-MM-dd");
  private static final Pattern escapeChars = Pattern.compile("[\t\n\r]");
  private static final String TSV_EXTENSION = ".tsv";
  private static final String YES = "YES";
  private static final String PNMC = "Participant Node Managers Committee";
  private static final String FILENAME_INVALID = "onlineButInvalid";
  private static final String FILENAME_2017 = "toRescueIn2017";
  private static final String FILENAME_2018 = "toRescueIn2018";
  private static final int PAGING_LIMIT = 100;
  // timeout in milliseconds for both the connection timeout and the response read timeout
  private static final int TIMEOUT_MILLIS = 500;

  private final DatasetService datasetService;
  private final OrganizationService organizationService;
  private final NodeService nodeService;
  private final InstallationService installationService;
  private final CubeService occurrenceCubeService;
  private final DatasetMetricsService datasetMetricsService;
  private final OccurrenceSearchService occurrenceSearchService;
  private final DatasetProcessStatusService statusService;

  private Map<String, List<String[]>> orphansByParticipant;
  private List<String[]> orphansRescued2017;
  private List<String[]> orphansRescued2018;
  private List<String[]> nonOrphansJustInvalid;
  private final File outputDirectory;

  OrphanDatasetScanner(DatasetService datasetService, OrganizationService organizationService, NodeService nodeService,
    InstallationService installationService, CubeService occurrenceCubeService,
    DatasetMetricsService datasetMetricsService, OccurrenceSearchService occurrenceSearchService,
    DatasetProcessStatusService statusService) throws IOException {
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;
    this.installationService = installationService;
    this.occurrenceCubeService = occurrenceCubeService;
    this.datasetMetricsService = datasetMetricsService;
    this.occurrenceSearchService = occurrenceSearchService;
    this.statusService = statusService;
    orphansByParticipant = Maps.newHashMap();
    orphansRescued2017 = Lists.newArrayList();
    orphansRescued2018 = Lists.newArrayList();
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

    // iterate through all datasets
    PagingResponse<Dataset> datasetResults;
    do {
      datasetResults = datasetService.list(datasetPage);
      for (Dataset d : datasetResults.getResults()) {
        if (datasets % 1000 == 0) {
          LOG.info("Iterated over " + datasets + " datasets");
        }
        datasets++;
        Organization organization = organizationService.get(d.getPublishingOrganizationKey());
        if (!toIgnore(d, organization)) {
          boolean orphaned = true;
          String mostRecentCrawlEndpointType = null;
          String mostRecentCrawlEndpointUri = null;
          String mostRecentCrawlStatus = null;
          String mostRecentCrawlDate = null;
          boolean potentialFalsePostitive = false;

          // iterate through the dataset's crawl history
          PagingResponse<DatasetProcessStatus> statusResults = null;
          PagingRequest statusPage = new PagingRequest(0, PAGING_LIMIT);
          do {
            try {
              statusResults = statusService.listDatasetProcessStatus(d.getKey(), statusPage);

              List<DatasetProcessStatus> results = statusResults.getResults();
              // check if latest crawl history has empty start date indicative of phantom crawl and potential false positive!
              potentialFalsePostitive = (results != null && !results.isEmpty() && results.get(0) != null
                                                        && results.get(0).getStartedCrawling() == null);
              // datasets that haven't been crawled yet, and registered before cutoff date are potential orphans
              if (statusResults.getCount()==0 && !beforeCutoff(d.getCreated())) {
                orphaned = true;
              } else {
                for (DatasetProcessStatus st : results) {
                  FinishReason finishReason = st.getFinishReason(); // NORMAL, USER_ABORT, ABORT, NOT_MODIFIED, UNKNOWN
                  Date finishedCrawling = st.getFinishedCrawling();
                  if (finishReason != null && finishedCrawling != null) {
                    if (mostRecentCrawlStatus == null) {
                      mostRecentCrawlStatus = finishReason.toString();
                      mostRecentCrawlDate = convertToIsoDate(finishedCrawling);
                      mostRecentCrawlEndpointType = (st.getCrawlJob().getEndpointType() == null) ? "" : st.getCrawlJob().getEndpointType().toString();
                      mostRecentCrawlEndpointUri = (st.getCrawlJob().getTargetUrl() == null) ? "" : st.getCrawlJob().getTargetUrl().toString();

                      // check: was most recent crawl successful, and did it occur before cutoff?
                      if (beforeCutoff(finishedCrawling)
                          && (finishReason.equals(FinishReason.NORMAL) || finishReason.equals(FinishReason.NOT_MODIFIED))) {
                        orphaned = false;
                        break;
                      }
                    }
                    // otherwise check: was this a successful crawl that occurred before cutoff?
                    else if (beforeCutoff(finishedCrawling)) {
                      if (finishReason != null && FinishReason.NORMAL.equals(finishReason)) {
                        orphaned = false;
                        break;
                      }
                    }
                  }
                }
              }
              // TODO: investigate why "739cf09a-d05e-4241-91ba-418b756f3ed5" throws Exception: org.codehaus.jackson.map.JsonMappingException: Instantiation of [simple type, class org.gbif.api.model.crawler.CrawlJob] value failed: null (through reference chain: org.gbif.api.model.common.paging.PagingResponse["results"]->org.gbif.api.model.crawler.DatasetProcessStatus["crawlJob"])
            } catch (Exception exception) {
              LOG.info("Failure iterating: Dataset " + d.getKey() + " Exception: " + exception.getMessage());
            }
            statusPage.nextPage();
          } while (statusResults != null && !statusResults.isEndOfRecords());

          // Warning: excluding false positives due to phantom crawl issue until it gets fixed: https://github.com/gbif/crawler/issues/3
          if (orphaned && !potentialFalsePostitive) {
            orphans++;
            Node node = nodeService.get(organization.getEndorsingNodeKey());
            String[] record =
              getRecord(d, organization, mostRecentCrawlEndpointType, mostRecentCrawlEndpointUri, mostRecentCrawlStatus,
                mostRecentCrawlDate, String.valueOf(potentialFalsePostitive).toUpperCase());

            // Warning: excluding false positives that are online but not indexed because they are invalid! Note that fixing https://github.com/gbif/crawler/issues/9 would help discover these.
            boolean online = Boolean.valueOf(record[20]); // corresponds to column "online?"
            int numOccurrences = Integer.valueOf(record[4]);
            int numUsages = Integer.valueOf(record[5]);
            if (online) {
              nonOrphansJustInvalid.add(record);
            }
            // Warning: excluding datasets that are offline with 0 records (0 occurrences or 0 name usages)
            else if (numOccurrences == 0 && numUsages == 0) {
              LOG.warn("Excluding dataset with 0 records: " + record[1]);
            }
            else {
              String key = (node.getParticipantTitle() == null) ? PNMC : node.getParticipantTitle();
              orphansByParticipant.computeIfAbsent(key, v -> Lists.newArrayList()).add(record);
              // split orphans into two files - to be rescued in 2018 (2nd round), and to be rescued in 2017 (1st round)
              if (rescueIn2018(organization)) {
                orphansRescued2018.add(record.clone());
              } else {
                orphansRescued2017.add(record.clone());
              }
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
    // write all orphans to be rescued in 2017 to file
    writeListToFiles(FILENAME_2017, orphansRescued2017);
    // write all orphans to be rescued in 2018 to file
    writeListToFiles(FILENAME_2018, orphansRescued2018);
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
                           + "[View](https://github.com/gbif/watchdog/blob/master/lists/orphansOct17/" + fileName
                           + ") / [Download](https://raw.githubusercontent.com/gbif/watchdog/master/lists/orphansOct17/"
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
   * @param potentialFalsePostitive     true if this dataset is potentially a false positive, false otherwise
   *
   * @return record as String array
   */
  @NotNull
  private String[] getRecord(Dataset dataset, Organization organization, String mostRecentCrawlEndpointType,
    String mostRecentCrawlEndpointUri, String mostRecentCrawlStatus, String mostRecentCrawlDate,
    String potentialFalsePostitive) {

    Installation installation = installationService.get(dataset.getInstallationKey());
    Node node = nodeService.get(organization.getEndorsingNodeKey());
    Country country = (node.getCountry() == null) ? Country.UNKNOWN : node.getCountry();

    // date dataset was registered in ISO 8601, to facilitate sorting
    String registered = convertToIsoDate(dataset.getCreated());

    // does the dataset have any endpoints?
    boolean hasEndpoints = dataset.getEndpoints().size() > 0;

    // how many occurrence records?
    long numOccurrences = occurrenceCubeService.get(new ReadBuilder().at(OccurrenceCube.DATASET_KEY, dataset.getKey()));

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
      mostRecentCrawlEndpointUri, mostRecentCrawlDate, mostRecentCrawlStatus, potentialFalsePostitive, YES,
      String.valueOf(imageCount), String.valueOf(online).toUpperCase()};
  }

  /**
   * Check if date occurred before cuttoff date.
   *
   * @param date cutoff date
   * @return true if date happened before cutoff date, false otherwise
   */
  private boolean beforeCutoff(@NotNull Date date) throws ParseException {
    Date cutoff = ISO_8601_SDF.parse(CUTOFF_DATE);
    return cutoff.compareTo(date) < 0;
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
    } catch (ClientHandlerException e) {
      LOG.error("Unable to retrieve image count for dataset" + datasetKey.toString(), e);
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
    // ignore Pangaea
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("d5778510-eb28-11da-8629-b8a03c50a862"))) {
      return true;
    }
    // ignore the UK
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("d897a5b9-35ee-4232-94bd-b0bcaac003c2"))) {
      return true;
    }
    // ignore the Catalogue of Life
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("f4ce3c03-7b38-445e-86e6-5f6b04b649d4"))) {
      return true;
    }
    // ignore GEO-Tag der Artenvielfalt - 1219 datasets with no crawl history, frozen in time
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("ef69a030-3940-11dd-b168-b8a03c50a862"))) {
      return true;
    }
    // ignore Plazi - more than 1600 false positives
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("ffb07bec-2d10-492d-9c37-361fe0b79427"))) {
      return true;
    }
    // ignore OBIS
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("ba0670b9-4186-41e6-8e70-f9cb3065551a"))) {
      return true;
    }
    // eBird, because it gets published once a year without fail
    if (dataset.getKey().equals(UUID.fromString("4fa7b334-ce0d-4e88-aaae-2e0c138d049e"))) {
      return true;
    }
    // ignore Togo, handled by BID
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("c9659a3e-07e9-4fcb-83c6-de8b9009a02e"))) {
      return true;
    }
    // ignore GBIFS
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("02c40d2a-1cba-4633-90b7-e36e5e97aba8"))) {
      return true;
    }
    return false;
  }

  /**
   * Checks if dataset should be rescued in second round of adoptions in 2018, to give the Node more time to
   * investigate their orphans. These are the Nodes that replied during the campaign.
   *
   * @param organization organization of candidate orphan dataset
   *
   * @return true if dataset should be rescued, false otherwise
   */
  private boolean rescueIn2018(Organization organization) {
    // Taiwan
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("e1b85abc-61f9-430f-ba79-6813dec53a0f"))) {
      return true;
    }
    // Switzerland
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("169eb292-376b-4cc6-8e31-9c2c432de0ad"))) {
      return true;
    }
    // Mexico
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("b324e8e9-9a4c-44fa-8f1a-7f39ea7ab576"))) {
      return true;
    }
    // Japan
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("db5705aa-6a6e-42f1-8942-d77b9f4896ea"))) {
      return true;
    }
    // Poland
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("2618bfdf-e194-4911-a241-80db3107bc51"))) {
      return true;
    }
    // New Zealand
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("4f9fd726-20dd-445e-89a1-8ed93792276f"))) {
      return true;
    }
    // Argentina
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("2426a871-9feb-40a1-96c6-0c593a76b835"))) {
      return true;
    }
    // Brazil
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("cdc9736d-5ff7-4ece-9959-3c744360cdb3"))) {
      return true;
    }
    // Chile
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("a8b16421-d80b-4ef3-8f22-098b01a89255"))) {
      return true;
    }
    // Colombia
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("7e865cba-7c46-417b-ade5-97f2cf5b7be0"))) {
      return true;
    }
    // Israel
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("8cb55387-7802-40e8-86d6-d357a583c596"))) {
      return true;
    }
    // CAS
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("e760dc6f-dd68-474d-ab41-bd3588571793"))) {
      return true;
    }
    // Germany
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("4f6826f2-4ff6-443d-b966-e6913bd24013"))) {
      return true;
    }
    // Sweden
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("a0b3be64-6525-4387-ac67-a499950f92e1"))) {
      return true;
    }
    // France
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("da44cd31-5901-4687-a106-6d1c7734ee3a"))) {
      return true;
    }
    // Denmark
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("4ddd294f-02b7-4359-ac33-0806a9ca9c6b"))) {
      return true;
    }
    // Portugal
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("673f7038-4262-4149-b753-5658a4e912f6"))) {
      return true;
    }
    // Norway
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("4f829580-180d-46a9-9c87-ed8ec959b545"))) {
      return true;
    }
    // Belgium
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("fb11cfe1-ebc3-45af-9159-17d9fddbcdac"))) {
      return true;
    }
    // Netherlands
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("0909d601-bda2-42df-9e63-a6d51847ebce"))) {
      return true;
    }
    // Finland
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("dd514216-5c7c-45f3-910d-797424cb5be6"))) {
      return true;
    }
    // Spain
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("1f94b3ca-9345-4d65-afe2-4bace93aa0fe"))) {
      return true;
    }
    // Participant Node Managers Committee - TODO communication with each individual publisher outstanding
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("7f48e0c8-5c96-49ec-b972-30748e339115"))) {
      return true;
    }
    // Andorra - TODO communication outstanding, as this Node's orphans didn't exist when campaign started
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("8df8d012-8e64-4c8a-886e-521a3bdfa623"))) {
      return true;
    }
    // India - TODO communication outstanding, as this Node's orphans didn't exist when campaign started
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("59d15a9b-ba51-43c3-a53f-304fe6732c04"))) {
      return true;
    }
    // Indonesia - TODO communication outstanding, as this Node's orphans didn't exist when campaign started
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("2ebd91f6-f14f-4375-b525-c011a3774f03"))) {
      return true;
    }
    // Ireland - TODO communication outstanding, as this Node's orphans didn't exist when campaign started
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("422b00b2-6c94-4bb6-976b-c359916efdb8"))) {
      return true;
    }
    return false;
  }


  /**
   * Method derived from https://stackoverflow.com/a/3584332 and tested in OrphanDatasetScannerTest.
   * Pings a HTTP URL with HEAD request and returns <code>true</code> if the response code is in 200-399 range.
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
    // http://choreutidae.lifedesks.org/classification.tar.gz HTTP installation from the USA dd247de0-f003-4d3e-b090-8b32c2c243da
    if (url.equalsIgnoreCase("http://choreutidae.lifedesks.org/classification.tar.gz")) {
      return false;
    }
    // http://data.aad.gov.au/digir/digir.php DiGIR installation from Australia 5ffda196-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://data.aad.gov.au/digir/digir.php")) {
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
    // http://dl.dropbox.com/u/523458/Dyntaxa/Archive.zip HTTP installation from Sweden 995f8ae4-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://dl.dropbox.com/u/523458/Dyntaxa/Archive.zip")) {
      return false;
    }
    // http://pensoft.net/dwc/bdj/checklist_* HTTP installation from BDJ d5b61ace-f25c-43bd-9dd0-03486850f90b
    if (url.startsWith("http://pensoft.net/dwc/bdj/checklist")) {
      return false;
    }
    // http://sammlung.pal.uni-erlangen.de/biocase/pywrapper.cgi?dsa=collectionpalaeobiology BioCASE installation from Germany 38290c67-22d0-4582-b288-641c29e913a2
    if (url.equalsIgnoreCase("http://sammlung.pal.uni-erlangen.de/biocase/pywrapper.cgi?dsa=collectionpalaeobiology")) {
      return false;
    }
    // http://sammlung.pal.uni-erlangen.de/biocase/pywrapper.cgi?dsa=herbariumerlangense BioCASE installation from Germany 38290c67-22d0-4582-b288-641c29e913a2
    if (url.equalsIgnoreCase("http://sammlung.pal.uni-erlangen.de/biocase/pywrapper.cgi?dsa=herbariumerlangense")) {
      return false;
    }
    // http://www.sib.gov.ar/tapirlink-0.7.0/www/tapir.php/APN-CHORDATA TAPIR installation from Argentina 6064fb16-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://www.sib.gov.ar/tapirlink-0.7.0/www/tapir.php/APN-CHORDATA")) {
      return false;
    }
    // http://www.sib.gov.ar/tapirlink-0.7.0/www/tapir.php/APN-DOCUMENTOS TAPIR installation from Argentina 6064fb16-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("http://www.sib.gov.ar/tapirlink-0.7.0/www/tapir.php/APN-DOCUMENTOS")) {
      return false;
    }
    // http://www.gbif.org.nz/tapirlink/tapir.php/NZBRN TAPIR installation from New Zealand that redirects to GBIF.org
    if (url.equalsIgnoreCase("http://www.gbif.org.nz/tapirlink/tapir.php/NZBRN")) {
      return false;
    }
    // https://herbarium.biology.colostate.edu/digir/DiGIR.php DiGIR installation from US 600b4684-f762-11e1-a439-00145eb45e9a
    if (url.equalsIgnoreCase("https://herbarium.biology.colostate.edu/digir/DiGIR.php")) {
      return false;
    }
    // http://diatoms.lifedesks.org/classification.tar.gz HTTP installation from US
    if (url.equalsIgnoreCase("http://diatoms.lifedesks.org/classification.tar.gz")) {
      return false;
    }
    url = url.replaceFirst("^https", "http"); // Otherwise an exception may be thrown on invalid SSL certificates.
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setConnectTimeout(TIMEOUT_MILLIS);
      connection.setReadTimeout(TIMEOUT_MILLIS);
      connection.setRequestMethod("HEAD");
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

  public static void main(String[] args) throws ParseException, IOException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    OrphanDatasetScanner scanner = new OrphanDatasetScanner(injector.getInstance(DatasetService.class),
      injector.getInstance(OrganizationService.class), injector.getInstance(NodeService.class),
      injector.getInstance(InstallationService.class), injector.getInstance(CubeService.class),
      injector.getInstance(DatasetMetricsService.class), injector.getInstance(OccurrenceSearchService.class),
      injector.getInstance(DatasetProcessStatusService.class));
    scanner.scan();
  }
}
