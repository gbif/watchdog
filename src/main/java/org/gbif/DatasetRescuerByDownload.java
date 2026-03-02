package org.gbif;

import freemarker.template.TemplateException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.http.StatusLine;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.Metadata;
import org.gbif.api.model.registry.Node;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.NodeService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.MetadataType;
import org.gbif.api.vocabulary.NodeType;
import org.gbif.metadata.eml.ipt.EmlFactory;
import org.gbif.metadata.eml.ipt.IptEmlWriter;
import org.gbif.metadata.eml.ipt.model.Agent;
import org.gbif.metadata.eml.ipt.model.Eml;
import org.gbif.metadata.eml.ipt.model.PhysicalData;
import org.gbif.utils.HttpClient;
import org.gbif.utils.HttpUtil;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.watchdog.config.WatchdogModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Class rescues orphan datasets by downloading them from GBIF.org in DwC-A format.
 */
public class DatasetRescuerByDownload {

  private static Logger LOG = LoggerFactory.getLogger(DatasetRescuerByDownload.class);

  // 10 second timeout
  private static final int CONNECTION_TIMEOUT_MSEC = 10000;
  private static final int MAX_CONNECTIONS = 1;
  private static final int MAX_PER_ROUTE = 1;
  private static final String GBIF_DOWNLOAD_EML = "metadata.xml";
  private static final String GBIF_DOWNLOAD_VERBATIM = "verbatim.txt";
  private static final String GBIF_DOWNLOAD_NAME = "GBIF Occurrence Download";

  private static final String RESCUED_EML = "eml.xml";
  private static final String RESCUED_OCCURRENCE = "rescued-occurrence.txt";
  private static final String RESCUED_META = "meta.xml";
  private static final String RESCUED_META_PATH = "/meta.xml";

  DownloadRequestService downloadRequestService;
  OccurrenceDownloadService occurrenceDownloadService;
  DatasetService datasetService;
  OrganizationService organizationService;
  NodeService nodeService;

  DatasetRescuerByDownload(DownloadRequestService occurrenceDownloadWsClient, OccurrenceDownloadService occurrenceDownloadService,
                           DatasetService datasetService, OrganizationService organizationService, NodeService nodeService) throws IOException {
    this.downloadRequestService = occurrenceDownloadWsClient;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.datasetService = datasetService;
    this.organizationService = organizationService;
    this.nodeService = nodeService;
  }

  /**
   * Downloads a dataset from GBIF.org in DwC-A format using its GBIF datasetKey.
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

    // Store the download key, so the download isn't repeated if this process is rerun.
    String downloadKey = datasetService.listMachineTags(uuid).stream()
      .filter(mt -> mt.getNamespace().equals("orphans.gbif.org") && mt.getName().equals("download"))
      .findFirst()
      .map(mt -> mt.getValue())
      .orElse(null);

    if (downloadKey == null) {
      // This isn't working.
      //EqualsPredicate p = new EqualsPredicate(OccurrenceSearchParameter.DATASET_KEY, datasetKey, false);
      //DownloadRequest request = new PredicateDownloadRequest(p, "MattBlissett", new HashSet(), true, DownloadFormat.DWCA, DownloadType.OCCURRENCE, null, null, null, null, null);
      //LOG.info("Download request {}", request);
      //downloadKey = downloadRequestService.create(request, null);

      URL url = new URL("https://api.gbif.org/v1/occurrence/download/request");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setRequestProperty("Content-Type", "application/json");
      con.setDoOutput(true);
      String auth = "MattBlissett:xxx";
      byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
      String authHeaderValue = "Basic " + new String(encodedAuth);
      con.setRequestProperty("Authorization", authHeaderValue);
      String requestString = "{\"predicate\":{\"type\":\"equals\",\"key\":\"DATASET_KEY\",\"value\":\"" + datasetKey + "\"},\"format\":\"DWCA\"}";
      try (OutputStream os = con.getOutputStream()) {
        byte[] input = requestString.getBytes("utf-8");
        os.write(input, 0, input.length);
      }

      try (BufferedReader br = new BufferedReader(
        new InputStreamReader(con.getInputStream(), "utf-8"))) {
        StringBuilder response = new StringBuilder();
        String responseLine = null;
        while ((responseLine = br.readLine()) != null) {
          response.append(responseLine.trim());
        }
        downloadKey = response.toString().trim();
        LOG.info("Download request {}", downloadKey);
      }

      MachineTag orphanDownload = new MachineTag();
      orphanDownload.setNamespace("orphans.gbif.org");
      orphanDownload.setName("download");
      orphanDownload.setValue(downloadKey);
      datasetService.addMachineTag(uuid, orphanDownload);

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
    }

    Download downloadMetadata;

    // proceed after download succeeds...
    do {
      LOG.info("Waiting for download [" + downloadKey + "] https://www.gbif.org/occurrence/download/" + downloadKey + " to complete...");
      Thread.sleep(500);
      downloadMetadata = occurrenceDownloadService.get(downloadKey); // try again
    } while (downloadMetadata == null || !downloadMetadata.isAvailable());

    LOG.info(downloadMetadata.getStatus().name());

    // retrieve download link, DOI and license
    LOG.info(downloadMetadata.getDoi().getDoiName());
    LOG.info(downloadMetadata.getDownloadLink());
    LOG.info(String.valueOf(downloadMetadata.getTotalRecords()));

    // Mark download to be kept forever
    if (downloadMetadata.getEraseAfter() != null) {
      downloadMetadata.setEraseAfter(null);
      occurrenceDownloadService.update(downloadMetadata);
    }

    // retrieve dataset metadata XML file from GBIF cache, e.g. http://api.gbif.org/v1/dataset/98333cb6-6c15-4add-aa0e-b322bf1500ba/document
    Eml eml;
    List<Metadata> metadata = datasetService.listMetadata(dataset.getKey(), MetadataType.EML);
    if (metadata.isEmpty()) {
      eml = EmlFactory.build(datasetService.getMetadataDocument(uuid));
    } else {
      eml = EmlFactory.build(datasetService.getMetadataDocument(metadata.get(0).getKey()));
    }

    HttpClient httpClient = HttpUtil.newMultithreadedClient(CONNECTION_TIMEOUT_MSEC, MAX_CONNECTIONS, MAX_PER_ROUTE);

    Path tmpDownloadDir = Files.createTempDirectory("orphan-download-");
    File tmpDwca = new File(tmpDownloadDir.toFile(), "rescued-dwca-" + datasetKey + ".zip");

    StatusLine status = httpClient.download(downloadMetadata.getDownloadLink(), tmpDwca);
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
      eml.setIntellectualRights(emlGbif.getIntellectualRights());
    }

    // publishing organisation
    Agent publishingOrg = new Agent();
    publishingOrg.setOrganisation(organization.getTitle());

    // we haven't republished this
    eml.setPubDate(null);

    // add external link to GBIF download (DwC-A format) that was used to rescue dataset - this must be preserved forever
    // First remove existing ones, in case this is a re-run of this script
    List<PhysicalData> toRemove = new ArrayList<>();
    for (PhysicalData pd : eml.getPhysicalData()) {
      if (pd.getName().equals(GBIF_DOWNLOAD_NAME)) {
        toRemove.add(pd);
      }
    }
    eml.getPhysicalData().removeAll(toRemove);
    PhysicalData physicalData = emlGbif.getPhysicalData().get(0);
    physicalData.setName(GBIF_DOWNLOAD_NAME);
    eml.addPhysicalData(physicalData);

    // ensure specimen preservation methods are lowercase, otherwise IPT doesn't recognize method
    ListIterator<String> iterator = eml.getSpecimenPreservationMethods().listIterator();
    while (iterator.hasNext()) {
      iterator.set(iterator.next().toLowerCase());
    }

    // remove "accessed via GBIF.org on YYYY-MM-DD." from citation if present.
    // (Matters for OBIS datasets, where the custom citation is used.)
    Pattern gbifCitationEnding = Pattern.compile(" accessed via GBIF.org on \\d\\d\\d\\d-\\d\\d-\\d\\d.");
    if (eml.getCitationString() != null) {
      String newCitation = gbifCitationEnding.matcher(eml.getCitationString()).replaceFirst("");
      LOG.info("New citation {}", newCitation);
      eml.getCitation().setCitation(newCitation);
    }

    // make DwC-A folder
    File dwcaFolder = Files.createTempDirectory("orphan-dwca-").toFile();

    // write eml.xml file to DwC-A folder
    File updatedEml = new File(dwcaFolder, RESCUED_EML);
    IptEmlWriter.writeEmlFile(updatedEml, eml);

    // retrieve verbatim.txt file, and copy to DwC-A folder
    File rescuedOccurrence = new File(dwcaFolder, RESCUED_OCCURRENCE);
    checkForDuplicateTriplesAndFixThem(new File(tmpDecompressDir.toFile(), GBIF_DOWNLOAD_VERBATIM), rescuedOccurrence);

    // retrieve meta.xml file, and copy to DwC-A folder
    File rescuedMeta = new File(dwcaFolder, RESCUED_META);
    FileUtils.copyInputStreamToFile(DatasetRescuerByDownload.class.getResourceAsStream(RESCUED_META_PATH), rescuedMeta);

    // make IPT resource directory
    File outputDir = new File("./", datasetKey);
    outputDir.mkdir();

    // write compressed (.zip) DwC-A file version 1.0 to IPT resource folder
    File versionedDwca = new File(outputDir, datasetKey.toString() + ".zip");
    CompressionUtil.zipDir(dwcaFolder, versionedDwca);

    LOG.info("DWCA folder: " + outputDir.getAbsolutePath());

    FileUtils.deleteDirectory(tmpDecompressDir.toFile());
    FileUtils.deleteDirectory(tmpDownloadDir.toFile());
    FileUtils.deleteDirectory(dwcaFolder);

    // Register dataset
    final String subdir;
    if (node.getType() == NodeType.COUNTRY) {
      subdir = organization.getCountry().getIso2LetterCode().toUpperCase();
    } else {
      subdir = organization.getEndorsingNodeKey().toString();
    }

    ProcessBuilder mkdirProcessBuilder = new ProcessBuilder();
    mkdirProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    mkdirProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    mkdirProcessBuilder.command("ssh", "mblissett@orphans.gbif.org",
      "mkdir", "-p", "/var/www/html/orphans.gbif.org/"+subdir);

    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.command("scp", "-p",
      versionedDwca.toString(),
      "mblissett@orphans.gbif.org:/var/www/html/orphans.gbif.org/"+subdir+"/"+datasetKey+".zip");

    try {
      Process mkdirProcess = mkdirProcessBuilder.start();
      mkdirProcess.waitFor();
      int mkdirExitVal = mkdirProcess.waitFor();
      if (mkdirExitVal != 0) {
        LOG.error("Failed to mkdir {}", subdir);
        return;
      }

      Process process = processBuilder.start();
      int exitVal = process.waitFor();
      if (exitVal == 0) {
        LOG.info("Copied {}.dwca to https://orphans.gbif.org/{}/{}.zip", datasetKey, subdir, datasetKey);
      } else {
        LOG.error("Failed to {} to https://orphans.gbif.org/{}/{}.zip, scp exited with {}", versionedDwca, datasetKey, subdir, datasetKey, exitVal);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    /*
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
    */
  }

  public static void main(String... args) throws Exception {
    WatchdogModule watchdogModule = new WatchdogModule();

    DatasetRescuerByDownload rescuer = new DatasetRescuerByDownload(
      watchdogModule.setupDownloadRequestService(),
      watchdogModule.setupOccurrenceDownloadService(),
      watchdogModule.setupDatasetService(),
      watchdogModule.setupOrganizationService(),
      watchdogModule.setupNodeService());

    List<String> datasets = new ArrayList<>();
    datasets.add("818a01e2-f762-11e1-a439-00145eb45e9a");
    datasets.add("3c6106dd-2948-4bce-894e-e5f8258cc5e7");
    datasets.add("d40e93a3-63fe-4c2f-be30-af21d4b66a95");
    datasets.add("8192ba94-f762-11e1-a439-00145eb45e9a");
    datasets.add("8259b55e-f762-11e1-a439-00145eb45e9a");
    datasets.add("7e5275e0-f762-11e1-a439-00145eb45e9a");
    datasets.add("3fb7ddd8-07c0-490d-a0f1-32b5dec8d583");
    datasets.add("dd047c25-da08-4a26-9490-52252dbdfad7");
    datasets.add("cfb98735-6065-42fa-8613-fce70d39cbbe");
    datasets.add("88435d97-186e-4435-98a6-52502aef0a79");
    datasets.add("c8ec4cc4-c9cf-431e-9390-d41ad30cb1ff");
    datasets.add("90fa011c-9f9c-476b-b50c-c1ce75a9d5bf");
    datasets.add("8ac24ec8-1e7b-40a9-a5b4-c8e3037a3431");
    datasets.add("34585f24-1ffe-4744-8734-7d563c918d18");
    datasets.add("6ba15d13-dd67-4d24-aa91-bf6a26cd7181");
    datasets.add("dd83b173-0b9f-4262-a49d-c72982965e69");
    datasets.add("3b0c24f6-faa0-4f3c-b3c2-89a73bf9cb48");
    datasets.add("0409a304-cc5d-4af6-b509-5b6667905117");
    datasets.add("824923c4-f762-11e1-a439-00145eb45e9a");
    datasets.add("de9feacc-af31-4a63-b439-72b84fcf40ad");
    datasets.add("91bdce11-da40-41b6-a042-ed56fb1f7fc1");
    datasets.add("12a6bf1f-f66e-408c-8c6c-771af210e6a8");
    datasets.add("f46c2ce0-8853-4c93-9922-63f850413809");
    datasets.add("800489ed-b266-4c34-8495-355537068fb1");
    datasets.add("e381b970-9b62-4664-a249-6023b6fa8ef9");
    datasets.add("dcf2e144-9956-491f-a5ac-8990c7fea3b9");
    datasets.add("5c4a1973-2b2f-4c32-a9c8-65150ef4cf13");
    datasets.add("d4eb19bc-fdce-415f-9a61-49b036009840");
    datasets.add("33614778-513a-4ec0-814d-125021cca5fe");
    datasets.add("8c468e30-1dcf-400e-9606-7ff4bf2c6e58");
    datasets.add("a5dd8cc3-7971-4b3c-a75c-d345940572bd");
    datasets.add("a6058f47-15ae-412e-b316-912dd6e2ec9b");
    datasets.add("33614778-513a-4ec0-814d-125021cca5fe");

    for (String datasetKey : datasets) {
      LOG.info("Rescuing {}", datasetKey);
      rescuer.rescue(datasetKey);
    }
  }

  /**
   * Deduplicate records using occurrenceId, institutionCode, collectionCode, catalogueNumber.
   */
  Predicate<String> duplicateTripleFilter = new Predicate<String>() {
    final String RS = "\u001e";

    @Override
    public boolean test(String s) {
      String[] columns = s.split("\t");
      String triple = columns[14-1] + RS + columns[15-1] + RS + columns[23-1] + RS + columns[22-1];
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

  Set<String> triples = new HashSet<>();

  private void checkForDuplicateTriplesAndFixThem(File verbatimFile, File rescueFile) {
    triples = new HashSet<>();

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
