package com.cognite.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import java.util.concurrent.Callable;

@Command(name = "files",
        description = "Operations on the CDF Files resource type",
        subcommands = {FileUpload.class, FileDelete.class})
public class FileParent implements Callable<Integer> {
    private static Logger LOG = LoggerFactory.getLogger(FileParent.class);

    @Override
    public Integer call() throws Exception {
        return 1;
    }
}
