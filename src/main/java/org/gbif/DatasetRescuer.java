package org.gbif;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
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
import org.gbif.api.model.registry.Comment;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.metadata.eml.*;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.*;

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

  DatasetRescuer(DownloadRequestService occurrenceDownloadWsClient, OccurrenceDownloadService occurrenceDownloadService,
    DatasetService datasetService, OrganizationService organizationService) throws IOException {
    this.downloadRequestService = occurrenceDownloadWsClient;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.datasetService = datasetService;
    this.organizationService = organizationService;

    FTL.setTimeZone(TimeZone.getTimeZone("GMT"));
    FTL.setDateTimeFormat("yyyy-MM-dd HH:mm:ss zzz");
  }

  /**
   * Downloads a dataset from GBIF.org in DwC-A format using its GBIF datasetKey.
   *
   * @param datasetKey GBIF datasetKey (UUID)
   */
  private void rescue(String datasetKey)
    throws IOException, ParserConfigurationException, SAXException, TemplateException, NoSuchFieldException,
    InterruptedException {

    // Katia Cezón GBIF Spain
    Agent rescuer = new Agent();
    rescuer.setFirstName("Katia");
    rescuer.setLastName("Cezón");
    rescuer.setEmail("katia@gbif.es");
    rescuer.setOrganisation("GBIF Spain");

    String downloadKey;

    // Store the download key, so the download isn't repeated if this process is rerun.
    File downloadIdFile = new File(datasetKey+".download");
    if (downloadIdFile.exists()) {
      downloadKey = Files.readFirstLine(downloadIdFile, Charset.defaultCharset());
    }
    else {
      EqualsPredicate p = new EqualsPredicate(OccurrenceSearchParameter.DATASET_KEY, datasetKey);
      DownloadRequest request = new DownloadRequest(p, "MattBlissett", Sets.newHashSet(), true, DownloadFormat.DWCA);
      downloadKey = downloadRequestService.create(request);
      Files.write(downloadKey, downloadIdFile, Charset.defaultCharset());

      Comment datasetComment = new Comment();
      datasetComment.setContent("Identified as orphaned and exported for rescue.");
      datasetService.addComment(UUID.fromString(datasetKey), datasetComment);
    }

    Download downloadMetadata;

    // proceed after download succeeds...
    do {
      LOG.info("Waiting for download [" + downloadKey + "] to complete...");
      Thread.sleep(500);
      downloadMetadata = occurrenceDownloadService.get(downloadKey); // try again
    } while (downloadMetadata == null || !downloadMetadata.isAvailable());

    LOG.info(downloadMetadata.getStatus().name());

    // retrieve download link, DOI and license
    LOG.info(downloadMetadata.getDoi().getDoiName());
    LOG.info(downloadMetadata.getDownloadLink());
    LOG.info(String.valueOf(downloadMetadata.getTotalRecords()));

    // retrieve dataset metadata XML file from GBIF cache, e.g. http://api.gbif.org/v1/dataset/98333cb6-6c15-4add-aa0e-b322bf1500ba/document
    Eml eml = EmlFactory.build(datasetService.getMetadataDocument(UUID.fromString(datasetKey)));

    DefaultHttpClient httpClient = HttpUtil.newMultithreadedClient(CONNECTION_TIMEOUT_MSEC, MAX_CONNECTIONS, MAX_PER_ROUTE);
    HttpUtil httpUtil = new HttpUtil(httpClient);

    File tmpDownloadDir = Files.createTempDir();
    File tmpDwca = new File(tmpDownloadDir, "rescued-dwca-" + datasetKey + ".zip");

    StatusLine status = httpUtil.download(downloadMetadata.getDownloadLink(), tmpDwca);
    LOG.info(status.getReasonPhrase() + " Check: " + tmpDwca.getAbsolutePath());

    File tmpDecompressDir = Files.createTempDir();
    CompressionUtil.decompressFile(tmpDecompressDir, tmpDwca, true);
    LOG.info("Unzipped to: " + tmpDecompressDir.getAbsolutePath());

    // retrieve dataset metadata XML file generated by GBIF
    InputStream emlGbifIs = new FileInputStream(new File(tmpDecompressDir, GBIF_DOWNLOAD_EML));
    Eml emlGbif = EmlFactory.build(emlGbifIs);
    LOG.info(emlGbif.getPhysicalData().toString());

    // ensure license is set!
    if (eml.parseLicenseUrl() == null) {
      throw new NoSuchFieldException("License must always be set!");
    }

    // publishing organisation
    Agent publishingOrg = new Agent();
    Dataset dataset = datasetService.get(UUID.fromString(datasetKey));
    Organization organization = organizationService.get(dataset.getPublishingOrganizationKey());
    publishingOrg.setOrganisation(organization.getTitle());

    // add up-to-date point of contact thereby also fulfilling minimum requirement
    eml.setContacts(Arrays.asList(rescuer));

    // add up-to-date creator thereby also fulfilling minimum requirement in order of priority high to low
    eml.setCreators(Arrays.asList(publishingOrg, rescuer));

    // add up-to-date metadata provider thereby also fulfilling minimum requirement
    eml.setMetadataProviders(Arrays.asList(rescuer));

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

    // wipe resource logo, to avoid calling broken links
    eml.setLogoUrl(null);

    // reset version to 1.0
    eml.setEmlVersion(1, 0);

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

    // add paragraph to description, explaining that this dataset has been rescued by scraping it from GBIF.org
    eml.getDescription().add(RESCUED_DISCLAIMER);

    // make DwC-A folder
    File dwcaFolder = Files.createTempDir();

    // write eml.xml file to DwC-A folder
    File updatedEml = new File(dwcaFolder, RESCUED_EML);
    EmlWriter.writeEmlFile(updatedEml, eml);

    // retrieve verbatim.txt file, and copy to DwC-A folder
    File rescuedOccurrence = new File(dwcaFolder, RESCUED_OCCURRENCE);
    FileUtils.copyFile(new File(tmpDecompressDir, GBIF_DOWNLOAD_VERBATIM), rescuedOccurrence);

    // retrieve meta.xml file, and copy to DwC-A folder
    File rescuedMeta = new File(dwcaFolder, RESCUED_META);
    FileUtils.copyInputStreamToFile(DatasetRescuer.class.getResourceAsStream(RESCUED_META_PATH), rescuedMeta);

    // upload to IPT

    // make IPT resource directory
    File iptResourceDir = new File("./", datasetKey);
    iptResourceDir.mkdir();

    // ensure publishing organisation set (prerequisite being the organisation and user kbraak@gbif.org must be added to the IPT before it can be loaded)
    // ensure auto-generation of citation turned on
    // make its visibility registered by default
    File resourceXml = new File(iptResourceDir, IPT_RESOURCE);
    writeIptResourceFile(resourceXml, dataset, downloadMetadata, rescuer, rescuedOccurrence.length());

    // make sources folder in IPT resource folder
    File sources = new File(iptResourceDir, IPT_SOURCES);
    sources.mkdir();

    // retrieve verbatim.txt file, and copy to IPT sources folder
    FileUtils.copyFile(new File(tmpDecompressDir, GBIF_DOWNLOAD_VERBATIM), new File(sources, RESCUED_OCCURRENCE));

    // write eml.xml file to IPT resource folder
    File iptEml = new File(iptResourceDir, RESCUED_EML);
    EmlWriter.writeEmlFile(iptEml, eml);

    // write eml.xml file version 1.0 to IPT resource folder
    File versionedEml = new File(iptResourceDir, VERSIONED_EML);
    EmlWriter.writeEmlFile(versionedEml, eml);

    // write compressed (.zip) DwC-A file version 1.0 to IPT resource folder
    File versionedDwca = new File(iptResourceDir, VERSIONED_DWCA);
    CompressionUtil.zipDir(dwcaFolder, versionedDwca);

    LOG.info("IPT Resource folder: " + iptResourceDir.getAbsolutePath());
  }

  public static void main(String... args)
    throws ParseException, IOException, ParserConfigurationException, SAXException, TemplateException,
    NoSuchFieldException, InterruptedException {
    Injector injector = Guice.createInjector(new WatchdogModule());
    DatasetRescuer rescuer = new DatasetRescuer(injector.getInstance(DownloadRequestService.class),
      injector.getInstance(OccurrenceDownloadService.class), injector.getInstance(DatasetService.class),
      injector.getInstance(OrganizationService.class));

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
    Map<String, Object> map = Maps.newHashMap();
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
   * Provides a freemarker template loader. It is configured to access the utf8 ipt folder on the classpath.
   */
  private static Configuration provideFreemarker() {
    TemplateLoader tl = new ClassTemplateLoader(DatasetRescuer.class, "/ipt");
    Configuration fm = new Configuration();
    fm.setDefaultEncoding("utf8");
    fm.setTemplateLoader(tl);
    return fm;
  }
}
