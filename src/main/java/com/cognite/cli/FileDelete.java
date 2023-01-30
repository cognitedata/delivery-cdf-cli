package com.cognite.cli;

import com.cognite.client.Request;
import com.cognite.client.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
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
    private Map<String, Object> filter;

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

        int totalDeleteCounter = 0;

        if (null != fileIds && fileIds.length > 0) {
            LOG.info("Start deleting files based on id...");
            List<Item> deleteIdItems = Arrays.stream(fileIds)
                    .mapToObj(id -> Item.newBuilder().setId(id).build())
                    .toList();

            cogClientMixin.getCogniteClient()
                    .files()
                    .delete(deleteIdItems);

            totalDeleteCounter += fileIds.length;
            LOG.info("Completed deleting {} files based on ids: {}",
                    fileIds.length,
                    fileIds);
        }

        if (null != fileExternalIds && fileExternalIds.length > 0) {
            LOG.info("Start deleting files based on external id...");
            List<Item> deleteExtIdItems = Arrays.stream(fileExternalIds)
                    .map(extId -> Item.newBuilder().setExternalId(extId).build())
                    .toList();

            cogClientMixin.getCogniteClient()
                    .files()
                    .delete(deleteExtIdItems);

            totalDeleteCounter += fileExternalIds.length;
            LOG.info("Completed deleting {} files based on external ids: {}",
                    fileExternalIds.length,
                    fileExternalIds);
        }

        if (null != filter && filter.size() > 0 || null != metadataFilter && metadataFilter.size() > 0) {
            LOG.info("Start deleting files based on filter...");

            // Build the request to filter files
            Request request = Request.create();
            for (Map.Entry<String, Object> entry : filter.entrySet()) {
                request = request.withFilterParameter(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : metadataFilter.entrySet()) {
                request = request.withFilterMetadataParameter(entry.getKey(), entry.getValue());
            }

            LOG.info("Matching files for request:\n {}", request);

            List<FileMetadata> fileFilterResults = new ArrayList<>();
            cogClientMixin.getCogniteClient().files()
                    .list(request)
                    .forEachRemaining(fileFilterResults::addAll);
            LOG.info("Found {} files matching the filter.", fileFilterResults.size());

            // Convert the file filter results to items to prepare for delete operation
            List<Item> deleteIdItems = fileFilterResults.stream()
                    .map(fileMetadata -> Item.newBuilder().setId(fileMetadata.getId()).build())
                    .toList();

            cogClientMixin.getCogniteClient()
                    .files()
                    .delete(deleteIdItems);

            totalDeleteCounter += deleteIdItems.size();
            LOG.info("Completed deleting {} files based on filter.",
                    deleteIdItems.size());
        }

        LOG.info("File deletion completed. {} files deleted.", totalDeleteCounter);
        return 0;
    }
}
