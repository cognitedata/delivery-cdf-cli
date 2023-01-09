package com.cognite.cli;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

import com.cognite.client.dto.*;

import com.cognite.client.queue.UploadQueue;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "upload",
        description = "Uploads a set of files to Cognite Data Fusion")
public class FileUpload implements Callable<Integer> {
    private static Logger LOG = LoggerFactory.getLogger(FileUpload.class);
    
    // global data structures
    private OptionalLong dataSetIntId;

    @CommandLine.Mixin
    private CogClientMixin cogClientMixin;

    @Parameters(index = "0", description = "The file(s) to upload.")
    private Path inputPath;

    @Option(names = {"-d", "--data-set-id"}, description = "The data set ID to upload files to.", arity = "0..1",
            interactive = true, echo = true, defaultValue = "-1")
    private long dataSetId;

    @Option(names = {"--data-set-ext-id"}, description = "The data set external ID to upload files to.",
            arity = "0..1", interactive = true, echo = true)
    private String dataSetExtId;

    @Option(names = "--ext-id-prefix", description = "An external id prefix to add to each file.",
            arity = "0..1", interactive = true, echo = true, defaultValue = "")
    private String extIdPrefix;

    @Option(names = "--file-directory", description = "The CDF file directory to upload the files to.",
            arity = "0..1", interactive = true, echo = true)
    private String fileDirectory;

    @Option(names = "--source", description = "The file metadata source value.", arity = "0..1", interactive = true,
            echo = true, defaultValue = "file-upload-cli")
    private String fileSource;
    
    @Override
    public Integer call() throws Exception {
        // Check that the input path exits
        if (!Files.exists(inputPath) || !Files.isReadable(inputPath)) {
            String message = String.format("Error: The specified input path does not exist or is not readable: %s", inputPath);
            LOG.error(message);
            throw new Exception(message);
        }

        LOG.info("Setting up the Cognite client and file upload queue.");
        UploadQueue<FileContainer, FileMetadata> fileBinaryUploadQueue = cogClientMixin.getCogniteClient().files().fileContainerUploadQueue()
                .withMaxUploadInterval(Duration.ofSeconds(5))
                .withPostUploadFunction(fileMetadataList ->
                        fileMetadataList.stream().forEach(fileMetadata -> LOG.info("Finished uploading {}.", fileMetadata.getName())))
                .withExceptionHandlerFunction(exception -> LOG.warn("Error during upload: {}", exception.getMessage()));
        fileBinaryUploadQueue.start();

        LOG.info("Start reading files...");
        int fileCounter = 0;
        // If the input path is a single file
        if (Files.isRegularFile(inputPath)) {
            LOG.info("The input path {} is a single file.", inputPath.toString());
            fileBinaryUploadQueue.put(buildFileContainer(inputPath));
            LOG.info("{} added to the upload queue.", inputPath.toString());
            fileCounter++;
        }

        // If the input path is a directory, traverse it. Do not walk recursively.
        if (Files.isDirectory(inputPath)) {
            LOG.info("The input path {} is a directory. Will traverse it.", inputPath.toString());
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(inputPath)) {
                for (Path entry: stream) {
                    if (Files.isRegularFile(entry)) {
                        fileBinaryUploadQueue.put(buildFileContainer(entry));
                        LOG.info("{} added to the upload queue.", entry.toString());
                        fileCounter++;
                    } else {
                        LOG.info("{} is not a regular file. Skipping.", entry);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error when traversing the input directory: {}", e.toString());
                throw e;
            }
        }

        // Stop the upload queue. This will also perform a final upload.
        fileBinaryUploadQueue.stop();
        LOG.info("File upload completed. {} files uploaded.", fileCounter);
        return 0;
    }

    /*
    Builds the file metadata and file container to prepare for file upload to Cognite Data Fusion.
     */
    private FileContainer buildFileContainer(Path path) throws Exception {
        // Build default metadata values
        FileMetadata.Builder metadataBuilder = FileMetadata.newBuilder()
                .setName(path.getFileName().toString())
                .setExternalId(extIdPrefix + path.getFileName().toString())
                .setSource(fileSource);

        if (null != fileDirectory && !fileDirectory.isBlank())
            metadataBuilder.setDirectory(fileDirectory);

        getDataSetIntId().ifPresent(dsId -> metadataBuilder.setDataSetId(dsId));

        // The file countainer builder
        FileContainer.Builder containerBuilder = FileContainer.newBuilder();

        if (Files.isReadable(path) && Files.isRegularFile(path)) {
            // Add the file binary
            containerBuilder.setFileBinary(FileBinary.newBuilder().setBinary(ByteString.copyFrom(Files.readAllBytes(path))));
        } else {
            LOG.info("{} is directory or not readable. Building and empty file container.", path.toString());
        }

        return containerBuilder.setFileMetadata(metadataBuilder.build()).build();
    }

    /*
    Return the data set internal id.

    If the data set external id has been configured, this method will translate this to the corresponding
    internal id.
     */
    private OptionalLong getDataSetIntId() throws Exception {
        if (null == dataSetIntId) {
            if (dataSetId != -1) {
                dataSetIntId = OptionalLong.of(dataSetId);
            } else if (null != dataSetExtId) {
                // Get the data set id
                LOG.info("Looking up the data set external id: {}.",
                        dataSetExtId);
                List<DataSet> dataSets = cogClientMixin.getCogniteClient().datasets()
                        .retrieve(List.of(Item.newBuilder().setExternalId(dataSetExtId).build()));

                if (dataSets.size() != 1) {
                    // The provided data set external id cannot be found.
                    String message = String.format("The configured data set external id does not exist: %s", dataSetExtId);
                    LOG.error(message);
                    throw new Exception(message);
                }
                dataSetIntId = OptionalLong.of(dataSets.get(0).getId());
            } else {
                dataSetIntId = OptionalLong.empty();
            }
        }

        return dataSetIntId;
    }
}
