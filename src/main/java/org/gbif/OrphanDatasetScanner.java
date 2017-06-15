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
import org.gbif.common.parsers.CountryParser;
import org.gbif.watchdog.config.WatchdogModule;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Program scans all datasets registered in GBIF for potential orphaned datasets.
 */
public class OrphanDatasetScanner {
  private static Logger LOG = LoggerFactory.getLogger(OrphanDatasetScanner.class);
  private static final String CUTOFF_DATE = "2016-11-16";
  private static final SimpleDateFormat ISO_8601_SDF = new SimpleDateFormat("yyyy-MM-dd");
  private static final Pattern escapeChars = Pattern.compile("[\t\n\r]");
  private static final String TSV_EXTENSION = ".tsv";
  private static final String YES = "YES";
  private static final String PNMC = "Participant Node Managers Committee";
  private static final int PAGING_LIMIT = 100;

  private final DatasetService datasetService;
  private final OrganizationService organizationService;
  private final NodeService nodeService;
  private final InstallationService installationService;
  private final CubeService occurrenceCubeService;
  private final DatasetMetricsService datasetMetricsService;
  private final OccurrenceSearchService occurrenceSearchService;
  private final DatasetProcessStatusService statusService;

  private Map<String, List<String[]>> orphansByParticipant;
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
        datasets++;
        Organization organization = organizationService.get(d.getPublishingOrganizationKey());
        if (!toIgnore(d, organization)) {
          boolean orphaned = true;
          String mostRecentCrawlEndpointType = null;
          String mostRecentCrawlEndpointUri = null;
          String mostRecentCrawlStatus = null;
          String mostRecentCrawlDate = null;
          String potentialFalsePostitive = null;

          // iterate through the dataset's crawl history
          PagingResponse<DatasetProcessStatus> statusResults = null;
          PagingRequest statusPage = new PagingRequest(0, PAGING_LIMIT);
          do {
            try {
              statusResults = statusService.listDatasetProcessStatus(d.getKey(), statusPage);

              List<DatasetProcessStatus> results = statusResults.getResults();
              // check if latest crawl history has empty start date indicative of phantom crawl and potential false positive!
              potentialFalsePostitive = String.valueOf((results != null && !results.isEmpty() && results.get(0) != null
                                                        && results.get(0).getStartedCrawling() == null)).toUpperCase();
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

          if (orphaned) {
            orphans++;
            Node node = nodeService.get(organization.getEndorsingNodeKey());
            String[] record = getRecord(d, organization, mostRecentCrawlEndpointType, mostRecentCrawlEndpointUri,
              mostRecentCrawlStatus, mostRecentCrawlDate, potentialFalsePostitive);
            String key = (node.getParticipantTitle() == null) ? PNMC : node.getParticipantTitle();
            orphansByParticipant.computeIfAbsent(key, v -> Lists.newArrayList()).add(record);
          }
        }
      } datasetPage.nextPage();
    } while (!datasetResults.isEndOfRecords());
    LOG.info("Total # of datasets: " + datasets);
    LOG.info("Total # of orphaned datasets: " + orphans);

    // write orphans to file, separated by participant
    writeMapToFiles(orphansByParticipant);
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
                           + "[View](https://github.com/kbraak/watchdog/blob/master/lists/orphans/" + fileName
                           + ") / [Download](https://raw.githubusercontent.com/kbraak/watchdog/master/lists/orphans/"
                           + fileName + ") |");
      } catch (IOException e) {
        LOG.error("Exception while writing to output file: " + out.getAbsolutePath());
      }
    }
    LOG.info("Total number of files written: " + files);
    LOG.info("Files written to: " + outputDirectory.getAbsolutePath());
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
        "mostRecentCrawlDate", "mostRecentCrawlStatus", "potentialFalsePositive?(YES/NO)","orphaned?(YES/NO)"};
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

    return new String[] {dataset.getTitle(), dataset.getKey().toString(), dataset.getType().toString(),
      String.valueOf(hasEndpoints), String.valueOf(numOccurrences), String.valueOf(numNameUsages),
      installation.getKey().toString(), installation.getType().toString(), organization.getKey().toString(),
      organization.getTitle(), node.getParticipantTitle(), country.getTitle(), registered, mostRecentCrawlEndpointType,
      mostRecentCrawlEndpointUri, mostRecentCrawlDate, mostRecentCrawlStatus, potentialFalsePostitive, YES};
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

  /**
   * Checks if dataset should be ignored for consideration as an orphan.
   *
   * @param dataset dataset
   * @param organization organization
   *
   * @return true if dataset should be skipped, false otherwise
   */
  private boolean toIgnore(Dataset dataset, Organization organization) {
    // from Pangaea?
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("d5778510-eb28-11da-8629-b8a03c50a862"))) {
      return true;
    }
    // from the UK?
    if (organization.getEndorsingNodeKey().equals(UUID.fromString("d897a5b9-35ee-4232-94bd-b0bcaac003c2"))) {
      return true;
    }
    // from Catalogue of Life?
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("f4ce3c03-7b38-445e-86e6-5f6b04b649d4"))) {
      return true;
    }
    // from GEO-Tag der Artenvielfalt - 1219 datasets with no crawl history, frozen in time?
    if (dataset.getPublishingOrganizationKey().equals(UUID.fromString("ef69a030-3940-11dd-b168-b8a03c50a862"))) {
      return true;
    }
    return false;
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
