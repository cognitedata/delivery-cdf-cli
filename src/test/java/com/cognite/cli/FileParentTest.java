package com.cognite.cli;

import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.FileMetadata;
import com.cognite.client.dto.Item;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FileParentTest {

    final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Test
    void singleFileUpload() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - singleFileUpload() -";
        String fileSource = "file-cli-test";

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "---------------  Start test. Upload a single file.  -----------------");

        String[] uploadInputArgs = {
                "files",
                "upload",
                "--cdf-project=" + TestConfigProvider.getProject(),
                "--cdf-host=" + TestConfigProvider.getHost(),
                "--client-id=" + TestConfigProvider.getClientId(),
                "--client-secret=" + TestConfigProvider.getClientSecret(),
                "--tenant-id=" + TestConfigProvider.getTenantId(),
                "--source=" + fileSource,
                "./files/aveva-class-library.xml"
        };

        CdfCli.main(uploadInputArgs);

        LOG.info(loggingPrefix + "-------------  Finished uploading single file. Duration : {} -------------",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "---------------- Clean up. Remove uploaded file  -----------------");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());

        List<FileMetadata> listFilesResults = new ArrayList<>();
        client.files()
                .list(Request.create()
                        .withFilterParameter("source", fileSource))
                .forEachRemaining(listFilesResults::addAll);

        List<Item> fileItems = listFilesResults.stream()
                .map(fileMetadata -> Item.newBuilder()
                        .setId(fileMetadata.getId())
                        .build())
                .toList();

        // build file id list argument for the cli
        String idParameter = "";
        for (Item item : fileItems) {
            idParameter += "--id=" + item.getId();
        }

        String[] deleteInputArgs = {
                "files",
                "delete",
                "--cdf-project=" + TestConfigProvider.getProject(),
                "--cdf-host=" + TestConfigProvider.getHost(),
                "--client-id=" + TestConfigProvider.getClientId(),
                "--client-secret=" + TestConfigProvider.getClientSecret(),
                "--tenant-id=" + TestConfigProvider.getTenantId(),
                idParameter
        };

        CdfCli.main(deleteInputArgs);

        //List<Item> deleteItemsResults = client.files().delete(fileItems);

        LOG.info(loggingPrefix + "------------  Finished removing the file. Duration : {} ------------",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(listFilesResults.size(), 1);
        //assertEquals(listFilesResults.size(), deleteItemsResults.size());
    }

    @Test
    void directoryUpload() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - directoryUpload() -";
        String fileSource = "file-cli-test";

        Path credentialsFile = Path.of("./test-cred-file.json");

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "---------------  Start test. Set up credentials file.  -----------------");

        // Construct the credentials file
        String credentialsFileString = """
                {
                    "cdfHost" : "%s",
                    "cdfProject" : "%s",
                    "clientId" : "%s",
                    "clientSecret" : "%s",
                    "aadTenantId" : "%s"
                }
                """.formatted(
                        TestConfigProvider.getHost(),
                        TestConfigProvider.getProject(),
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TestConfigProvider.getTenantId());

        Files.writeString(credentialsFile, credentialsFileString, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "---------------  Upload a directory.  -----------------");

        String[] uploadInputArgs = {
                "files",
                "upload",
                "--credentials-file=" + credentialsFile.toString(),
                "--source=" + fileSource,
                "./files"
        };

        CdfCli.main(uploadInputArgs);

        LOG.info(loggingPrefix + "-------------  Finished uploading directory. Duration : {} -------------",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "---------------- Clean up. Remove uploaded file  -----------------");
        CogniteClient client = CogniteClient.ofClientCredentials(
                        TestConfigProvider.getClientId(),
                        TestConfigProvider.getClientSecret(),
                        TokenUrl.generateAzureAdURL(TestConfigProvider.getTenantId()))
                .withProject(TestConfigProvider.getProject())
                .withBaseUrl(TestConfigProvider.getHost());

        List<FileMetadata> listFilesResults = new ArrayList<>();
        client.files()
                .list(Request.create()
                        .withFilterParameter("source", fileSource))
                .forEachRemaining(listFilesResults::addAll);

        List<Item> fileItems = listFilesResults.stream()
                .map(fileMetadata -> Item.newBuilder()
                        .setId(fileMetadata.getId())
                        .build())
                .toList();

        String[] deleteInputArgs = {
                "files",
                "delete",
                "--credentials-file=" + credentialsFile.toString(),
                "--filter source=\"" + fileSource + "\""
        };

        CdfCli.main(deleteInputArgs);

        //List<Item> deleteItemsResults = client.files().delete(fileItems);

        LOG.info(loggingPrefix + "------------  Finished removing the file. Duration : {} ------------",
                Duration.between(startInstant, Instant.now()));

        LOG.info(loggingPrefix + "---------------- Clean up. Remove credentials file  -----------------");
        Files.deleteIfExists(credentialsFile);

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

        assertEquals(listFilesResults.size(), 2);
        //assertEquals(listFilesResults.size(), deleteItemsResults.size());
    }

    @Test
    void invalidParameters() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - invalidParameters() -";
        String fileSource = "file-cli-test";

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "---------------  Start test. Invalid parameters.  -----------------");

        String[] inputArgs = {
                "files",
                "upload",
                "--cdf-project=" + TestConfigProvider.getProject(),
                "--cdf-host=" + TestConfigProvider.getHost(),
                "--client-id=" + TestConfigProvider.getClientId(),
                "--client-secret=" + TestConfigProvider.getClientSecret(),
                "--tenant-id=" + TestConfigProvider.getTenantId(),
                "--source=" + fileSource
        };

        CdfCli.main(inputArgs);

        LOG.info(loggingPrefix + "-------------  Finished test. Duration : {} -------------",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
    }

    @Test
    void missingParameters() throws Exception {
        Instant startInstant = Instant.now();
        String loggingPrefix = "UnitTest - missingParameters() -";

        LOG.info(loggingPrefix + "----------------------------------------------------------------------");
        LOG.info(loggingPrefix + "---------------  Start test. Missing parameters.  -----------------");

        String[] inputArgs = {
                "files",
                "upload",
                "--cdf-project= ",
                "--cdf-host= ",
                "--client-id= ",
                "--client-secret= ",
                "--tenant-id= ",
                "./files/"
        };

        CdfCli.main(inputArgs);

        LOG.info(loggingPrefix + "-------------  Finished test. Duration : {} -------------",
                Duration.between(startInstant, Instant.now()));
        LOG.info(loggingPrefix + "----------------------------------------------------------------------");

    }
}