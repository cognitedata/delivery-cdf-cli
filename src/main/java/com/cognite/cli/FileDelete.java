package com.cognite.cli;

import com.cognite.client.Request;
import com.cognite.client.dto.*;
import com.cognite.client.queue.UploadQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "delete",
        description = "Deletes a set of files from Cognite Data Fusion")
public class FileDelete implements Callable<Integer> {
    private static Logger LOG = LoggerFactory.getLogger(FileDelete.class);
    
    // global data structures

    @CommandLine.Mixin
    private CogClientMixin cogClientMixin;

    @Option(names = "--id", description = "The internal id of the files to delete.",
            arity = "0..1", interactive = true, echo = true, defaultValue = "")
    private long[] fileIds;

    @Option(names = {"--ext-id"}, description = "The external id of the files to delete.",
            arity = "0..1", interactive = true, echo = true)
    private String[] fileExternalIds;

    @Option(names = {"--filter"}, description = "A file filter expression in the format <key=value>. You can specify multiple filters",
            arity = "0..1", interactive = true, echo = true)
    private Map<String, String> filter;

    @Option(names = {"--metadata-filter"}, description = "A file metadata filter expression in the format <key=value>. You can specify multiple filters",
            arity = "0..1", interactive = true, echo = true)
    private Map<String, String> metadataFilter;

    @Override
    public Integer call() throws Exception {
        // Check that we have some input specified
        if (fileIds.length == 0 && fileExternalIds.length == 0 && filter.isEmpty() && metadataFilter.isEmpty()) {
            LOG.info("No file (external) ids specified nor any filter. No files to delete.");
            return 0;
        }

        if (fileIds.length > 0) {
            LOG.info("Start deleting files based on id...");
            List<Item> deleteIdItems = Arrays.stream(fileIds)
                    .mapToObj(id -> Item.newBuilder().setId(id).build())
                    .toList();

            cogClientMixin.getCogniteClient()
                    .files()
                    .delete(deleteIdItems);

            LOG.info("Completed deleting {} files based on ids: {}",
                    fileIds.length,
                    fileIds);
        }

        if (fileExternalIds.length > 0) {
            LOG.info("Start deleting files based on external id...");
            List<Item> deleteExtIdItems = Arrays.stream(fileExternalIds)
                    .map(extId -> Item.newBuilder().setExternalId(extId).build())
                    .toList();

            cogClientMixin.getCogniteClient()
                    .files()
                    .delete(deleteExtIdItems);

            LOG.info("Completed deleting {} files based on external ids: {}",
                    fileExternalIds.length,
                    fileExternalIds);
        }

        if (filter.size() > 0 || metadataFilter.size() > 0) {
            LOG.info("Start deleting files based on filter...");

            // Build the request to filter files
            Request request = Request.create();



            List<Item> deleteExtIdItems = Arrays.stream(fileExternalIds)
                    .map(extId -> Item.newBuilder().setExternalId(extId).build())
                    .toList();

            cogClientMixin.getCogniteClient()
                    .files()
                    .delete(deleteExtIdItems);

            LOG.info("Completed deleting {} files based on external ids: {}",
                    fileExternalIds.length,
                    fileExternalIds);
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


        LOG.info("File upload completed. {} files uploaded.", fileCounter);
        return 0;
    }
}
