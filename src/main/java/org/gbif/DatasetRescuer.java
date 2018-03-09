package org.gbif;

import com.google.inject.Guice;
import com.google.inject.Injector;
import freemarker.cache.ClassTemplateLoader;
import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.commons.io.FileUtils;
import org.apache.http.StatusLine;
import org.apache.http.impl.client.DefaultHttpClient;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.registry.*;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.NodeType;
import org.gbif.metadata.eml.*;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.gbif.DatasetRescuer.Mode.IPT_EXPORT;
import static org.gbif.DatasetRescuer.Mode.RESCUE;

/**
 * Class rescues orphan datasets by downloading them from GBIF.org in DwC-A format.
 */
public class DatasetRescuer {

  private static Logger LOG = LoggerFactory.getLogger(DatasetRescuer.class);

  // 10 second timeout
  private static final int CONNECTION_TIMEOUT_MSEC = 10000;
  private static final int MAX_CONNECTIONS = 1;
  private static final int MAX_PER_ROUTE = 1;
  private static final String GBIF_DOWNLOAD_EML = "metadata.xml";
  private static final String GBIF_DOWNLOAD_VERBATIM = "verbatim.txt";
  private static final String GBIF_DOWNLOAD_NAME = "GBIF Occurrence Download";
  private static final String RESCUED_DISCLAIMER = "Note: this dataset was previously orphaned. It has been rescued by ① extracting it from the GBIF.org index (see GBIF Download in External Data) and ② republishing it on this IPT data hosting centre as version 1.0.";

  private static final String RESCUED_EML = "eml.xml";
  private static final String RESCUED_OCCURRENCE = "occurrence.txt";
  private static final String RESCUED_META = "meta.xml";
  private static final String RESCUED_META_PATH = "/meta.xml";

  private static final String IPT_RESOURCE_TEMPLATE = "resource.ftl";
  private static final Configuration FTL = provideFreemarker();
  private static final String IPT_RESOURCE = "/resource.xml";
  private static final String IPT_SOURCES = "sources";
  private static final String VERSIONED_EML = "eml-1.0.xml";
  private static final String VERSIONED_DWCA = "dwca-1.0.zip";

  DownloadRequestService downloadRequestService;
  OccurrenceDownloadService occurrenceDownloadService;
  DatasetService datasetService;
  OrganizationService organizationService;
  NodeService nodeService;

  DatasetRescuer(DownloadRequestService occurrenceDownloadWsClient, OccurrenceDownloadService occurrenceDownloadService,
    DatasetService datasetService, OrganizationService organizationService, NodeService nodeService) throws IOException {
    this.downloadRequestService = occurrenceDownloadWsClient;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;

    FTL.setTimeZone(TimeZone.getTimeZone("GMT"));
    FTL.setDateTimeFormat("yyyy-MM-dd HH:mm:ss zzz");
  }

  enum Mode {
    RESCUE, // Just rescuing (fostering?) the orphan, a ZIP export and update the endpoint
    IPT_EXPORT // Export for publisher to adopt
  }

  /**
   * Downloads a dataset from GBIF.org in DwC-A format using its GBIF datasetKey.
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(String datasetKey)
    throws IOException, ParserConfigurationException, SAXException, TemplateException, NoSuchFieldException,
    InterruptedException, URISyntaxException {

    Mode mode = RESCUE;

    Agent rescuer = new Agent();

    if (mode == IPT_EXPORT) {
      // Katia Cezón GBIF Spain
      rescuer.setFirstName("Katia");
      rescuer.setLastName("Cezón");
      rescuer.setEmail("katia@gbif.es");
      rescuer.setOrganisation("GBIF Spain");
    }

    UUID uuid = UUID.fromString(datasetKey);
    Dataset dataset = datasetService.get(uuid);
    Organization organization = organizationService.get(dataset.getPublishingOrganizationKey());
    Node node = nodeService.get(organization.getEndorsingNodeKey());

    // Store the download key, so the download isn't repeated if this process is rerun.
    String downloadKey = datasetService.listMachineTags(uuid).stream()
        .filter(mt -> mt.getNamespace().equals("orphans.gbif.org") && mt.getName().equals("download"))
        .findFirst()
        .map(mt -> mt.getValue())
        .orElse(null);

    if (downloadKey == null) {
      EqualsPredicate p = new EqualsPredicate(OccurrenceSearchParameter.DATASET_KEY, datasetKey);
      DownloadRequest request = new DownloadRequest(p, "MattBlissett", new HashSet(), true, DownloadFormat.DWCA);
      downloadKey = downloadRequestService.create(request);

      MachineTag orphanDownload = new MachineTag();
      orphanDownload.setNamespace("orphans.gbif.org");
      orphanDownload.setName("download");
      orphanDownload.setValue(downloadKey);
      datasetService.addMachineTag(uuid, orphanDownload);

      MachineTag orphanExport = new MachineTag();
      orphanExport.setNamespace("orphans.gbif.org");
      orphanExport.setName("status");
      if (mode == RESCUE) {
        orphanExport.setValue("RESCUED");
      } else if (mode == IPT_EXPORT) {
        orphanExport.setValue("ADOPTED");
      }
      datasetService.addMachineTag(uuid, orphanExport);

      MachineTag orphanEndpoint = new MachineTag();
      orphanEndpoint.setNamespace("orphans.gbif.org");
      orphanEndpoint.setName("orphanEndpoint");
      orphanEndpoint.setValue(dataset.getEndpoints().get(0).getUrl().toString());
      datasetService.addMachineTag(uuid, orphanEndpoint);
    }

    Download downloadMetadata;

    // proceed after download succeeds...
    do {
      LOG.info("Waiting for download [" + downloadKey + "] to complete...");
      Thread.sleep(2500);
      downloadMetadata = occurrenceDownloadService.get(downloadKey); // try again
    } while (downloadMetadata == null || !downloadMetadata.isAvailable());

    LOG.info(downloadMetadata.getStatus().name());

    // retrieve download link, DOI and license
    LOG.info(downloadMetadata.getDoi().getDoiName());
    LOG.info(downloadMetadata.getDownloadLink());
    LOG.info(String.valueOf(downloadMetadata.getTotalRecords()));

    // Mark download to be kept forever
    downloadMetadata.setEraseAfter(null);
    occurrenceDownloadService.update(downloadMetadata);

    // retrieve dataset metadata XML file from GBIF cache, e.g. http://api.gbif.org/v1/dataset/98333cb6-6c15-4add-aa0e-b322bf1500ba/document
    Eml eml = EmlFactory.build(datasetService.getMetadataDocument(uuid));

    DefaultHttpClient httpClient = HttpUtil.newMultithreadedClient(CONNECTION_TIMEOUT_MSEC, MAX_CONNECTIONS, MAX_PER_ROUTE);
    HttpUtil httpUtil = new HttpUtil(httpClient);

    Path tmpDownloadDir = Files.createTempDirectory("orphan-download-");
    File tmpDwca = new File(tmpDownloadDir.toFile(), "rescued-dwca-" + datasetKey + ".zip");

    StatusLine status = httpUtil.download(downloadMetadata.getDownloadLink(), tmpDwca);
    LOG.info(status.getReasonPhrase() + " Check: " + tmpDwca.getAbsolutePath());

    Path tmpDecompressDir = Files.createTempDirectory("orphan-decompress-");
    CompressionUtil.decompressFile(tmpDecompressDir.toFile(), tmpDwca, true);
    LOG.info("Unzipped to: {}", tmpDecompressDir);

    // retrieve dataset metadata XML file generated by GBIF
    InputStream emlGbifIs = new FileInputStream(new File(tmpDecompressDir.toFile(), GBIF_DOWNLOAD_EML));
    Eml emlGbif = EmlFactory.build(emlGbifIs);
    LOG.info(emlGbif.getPhysicalData().toString());

    // ensure license is set!
    if (eml.parseLicenseUrl() == null) {
      throw new NoSuchFieldException("License must always be set!");
    }

    // publishing organisation
    Agent publishingOrg = new Agent();
    publishingOrg.setOrganisation(organization.getTitle());

    if (mode == IPT_EXPORT) {
      // add up-to-date point of contact thereby also fulfilling minimum requirement
      eml.setContacts(Arrays.asList(rescuer));

      // add up-to-date creator thereby also fulfilling minimum requirement in order of priority high to low
      eml.setCreators(Arrays.asList(publishingOrg, rescuer));

      // add up-to-date metadata provider thereby also fulfilling minimum requirement
      eml.setMetadataProviders(Arrays.asList(rescuer));
    }
    else if (mode == RESCUE) {
      // we haven't republished this
      eml.setPubDate(null);
    }

    // add external link to GBIF download (DwC-A format) that was used to rescue dataset - this must be preserved forever
    if (!emlGbif.getPhysicalData().isEmpty()) {
      PhysicalData physicalData = emlGbif.getPhysicalData().get(0);
      physicalData.setName(GBIF_DOWNLOAD_NAME);
      eml.addPhysicalData(physicalData);
    }

    // ensure specimen preservation methods are lowercase, otherwise IPT doesn't recognize method
    ListIterator<String> iterator = eml.getSpecimenPreservationMethods().listIterator();
    while (iterator.hasNext()) {
      iterator.set(iterator.next().toLowerCase());
    }

    // wipe resource logo, to avoid calling broken links, if it is broken
    if (eml.getLogoUrl() != null) {
      String logoUrl = "";
      try {
        HttpUtil.Response logoResponse = httpUtil.get(eml.getLogoUrl());
        if (logoResponse.getStatusCode() == 200) {
          logoUrl = eml.getLogoUrl();
        }
      } catch (Exception e) {}
      eml.setLogoUrl(logoUrl);
    }

    if (mode == IPT_EXPORT) {
      // reset version to 1.0
      eml.setEmlVersion(1, 0);
    }

    // Fix paragraphs in description.
    LOG.info("Description is {}, {}", eml.getDescription().size(), eml.getDescription());
    if (eml.getDescription().size() == 1) {
      String temp = eml.getDescription().get(0);
      eml.getDescription().clear();
      String[] paragraphs = temp.split("</?p>");
      for (String pp : paragraphs) {
        if (pp.trim().length() > 0) {
          eml.getDescription().add(pp.trim());
        }
      }
    } else {
    }

    if (mode == IPT_EXPORT) {
      // add paragraph to description, explaining that this dataset has been rescued by scraping it from GBIF.org
      eml.getDescription().add(RESCUED_DISCLAIMER);
    }

    // make DwC-A folder
    File dwcaFolder = Files.createTempDirectory("orphan-dwca-").toFile();

    // write eml.xml file to DwC-A folder
    File updatedEml = new File(dwcaFolder, RESCUED_EML);
    EmlWriter.writeEmlFile(updatedEml, eml);

    // retrieve verbatim.txt file, and copy to DwC-A folder
    File rescuedOccurrence = new File(dwcaFolder, RESCUED_OCCURRENCE);
    if (mode == RESCUE) {
      checkForDuplicateTriplesAndFixThem(new File(tmpDecompressDir.toFile(), GBIF_DOWNLOAD_VERBATIM), rescuedOccurrence);
    }
    else if (mode == IPT_EXPORT) {
      FileUtils.copyFile(new File(tmpDecompressDir.toFile(), GBIF_DOWNLOAD_VERBATIM), rescuedOccurrence);
    }

    // retrieve meta.xml file, and copy to DwC-A folder
    File rescuedMeta = new File(dwcaFolder, RESCUED_META);
    FileUtils.copyInputStreamToFile(DatasetRescuer.class.getResourceAsStream(RESCUED_META_PATH), rescuedMeta);

    // make IPT resource directory
    File iptResourceDir = new File("./", datasetKey);
    iptResourceDir.mkdir();

    if (mode == IPT_EXPORT) {
      // upload to IPT

      // ensure publishing organisation set (prerequisite being the organisation and user informatics@gbif.org must be added to the IPT before it can be loaded)
      // ensure auto-generation of citation turned on
      // make its visibility registered by default
      File resourceXml = new File(iptResourceDir, IPT_RESOURCE);
      writeIptResourceFile(resourceXml, dataset, downloadMetadata, rescuer, rescuedOccurrence.length());

      // make sources folder in IPT resource folder
      File sources = new File(iptResourceDir, IPT_SOURCES);
      sources.mkdir();

      // retrieve verbatim.txt file, and copy to IPT sources folder
      FileUtils.copyFile(rescuedOccurrence, new File(sources, RESCUED_OCCURRENCE));

      // write eml.xml file to IPT resource folder
      File iptEml = new File(iptResourceDir, RESCUED_EML);
      EmlWriter.writeEmlFile(iptEml, eml);

      // write eml.xml file version 1.0 to IPT resource folder
      File versionedEml = new File(iptResourceDir, VERSIONED_EML);
      EmlWriter.writeEmlFile(versionedEml, eml);
    }

    // write compressed (.zip) DwC-A file version 1.0 to IPT resource folder
    File versionedDwca = new File(iptResourceDir, VERSIONED_DWCA);
    CompressionUtil.zipDir(dwcaFolder, versionedDwca);

    LOG.info("IPT Resource / rescue folder: " + iptResourceDir.getAbsolutePath());

    FileUtils.deleteDirectory(tmpDecompressDir.toFile());
    FileUtils.deleteDirectory(tmpDownloadDir.toFile());
    FileUtils.deleteDirectory(dwcaFolder);

    if (mode == RESCUE) {
      // Update endpoint in the registry
      Endpoint oldEndpoint = dataset.getEndpoints().stream()
          .filter(ep -> ep.getType() == EndpointType.BIOCASE || ep.getType() == EndpointType.DIGIR || ep.getType() == EndpointType.DIGIR_MANIS || ep.getType() == EndpointType.BIOCASE || ep.getType() == EndpointType.DWC_ARCHIVE || ep.getType() == EndpointType.TAPIR)
          .findFirst()
          .orElse(null);

      if (oldEndpoint == null || !oldEndpoint.getUrl().toString().contains("orphans.gbif.org")) {
        if (oldEndpoint != null) {
          datasetService.deleteEndpoint(uuid, oldEndpoint.getKey());
        }

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
  }

  public static void main(String... args)
    throws ParseException, IOException, ParserConfigurationException, SAXException, TemplateException,
    NoSuchFieldException, InterruptedException, URISyntaxException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    DatasetRescuer rescuer = new DatasetRescuer(injector.getInstance(DownloadRequestService.class),
      injector.getInstance(OccurrenceDownloadService.class), injector.getInstance(DatasetService.class),
      injector.getInstance(OrganizationService.class), injector.getInstance(NodeService.class));

    if (args.length < 1) {
      System.err.println("Give dataset keys as argument");
      System.exit(1);
    }

    for (String datasetKey : args) {
      rescuer.rescue(datasetKey);
    }
  }

  /**
   * Writes an {@link Eml} object to an XML file using a Freemarker {@link Configuration}.
   *
   * @param f        the XML file to write to
   * @param dataset  the GBIF Dataset object
   * @param download the GBIF Download object
   */
  private void writeIptResourceFile(File f, Dataset dataset, Download download, Agent rescuer, long occurrenceFileSize) throws IOException, TemplateException {
    Map<String, Object> map = new HashMap();
    map.put("dataset", dataset);
    map.put("download", download);
    map.put("rescuer", rescuer);
    map.put("occurrenceFileSize", occurrenceFileSize);
    writeFile(f, IPT_RESOURCE_TEMPLATE, map);
  }

  /**
   * Writes a map of data to a utf8 encoded file using a Freemarker {@link Configuration}.
   */
  private void writeFile(File f, String template, Object data) throws IOException, TemplateException {
    String result = processTemplateIntoString(FTL.getTemplate(template), data);
    Writer out = org.gbif.utils.file.FileUtils.startNewUtf8File(f);
    out.write(result);
    out.close();
  }

  private String processTemplateIntoString(Template template, Object model) throws IOException, TemplateException {
    StringWriter result = new StringWriter();
    template.process(model, result);
    return result.toString();
  }

  /**
   * Provides a freemarker template loader. It is configured to access the UTF-8 IPT folder on the classpath.
   */
  private static Configuration provideFreemarker() {
    TemplateLoader tl = new ClassTemplateLoader(DatasetRescuer.class, "/ipt");
    Configuration fm = new Configuration();
    fm.setDefaultEncoding("utf8");
    fm.setTemplateLoader(tl);
    return fm;
  }

  /**
   * Deduplicate records using occurrenceId, institutionCode, collectionCode, catalogueNumber.
   */
  Predicate<String> duplicateTripleFilter = new Predicate<String>() {
    final String RS = "\u001e";
    Set<String> triples = new HashSet<>();

    @Override
    public boolean test(String s) {
      String[] columns = s.split("\t");
      String triple = columns[67] + RS + columns[59] + RS + columns[60] + RS + columns[68];
      if (triples.contains(triple)) {
        System.out.println("Duplicate triple "+triple.replace(RS, "\u241e"));
        return false;
      }
      triples.add(triple);
      return true;
    }
  };

  /*
   * Order the records so the header line is first, then the records in order of gbifID.
   * Some BioCASe providers had multiple identifications, which were imported as a "DWC Quad" — institution code,
   * collection code, catalogue number and identification qualifier.
   *
   * The primary identification was imported first, so keep the occurrence with the lowest gbifID.
   */
  Comparator<String> gbifKeyOrdering = new Comparator<String>() {
    @Override
    public int compare(String s, String t) {
      String sKey = s.split("\t")[0];
      String tKey = t.split("\t")[0];

      if (sKey.equals("gbifID")) return -1;
      if (tKey.equals("gbifID")) return +1;

      int sId = Integer.parseInt(sKey);
      int tId = Integer.parseInt(tKey);

      if (sId == tId) return 0;
      if (sId < tId) return -1;
      return +1;
    }
  };

  private void checkForDuplicateTriplesAndFixThem(File verbatimFile, File rescueFile) {

    try (Stream<String> stream = Files.lines(verbatimFile.toPath())) {
      try (PrintWriter pw = new PrintWriter(rescueFile, "UTF-8")) {
        stream.sorted(gbifKeyOrdering)
        .filter(duplicateTripleFilter)
        .forEachOrdered(
            pw::println
        );
      }
    }
    catch (IOException e) {}
  }
}
