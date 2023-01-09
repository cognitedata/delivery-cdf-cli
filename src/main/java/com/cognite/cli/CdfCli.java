package com.cognite.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "cdf",
        description = "The CDF CLI entry point.",
        subcommands = {FileParent.class})
public class CdfCli implements Callable<Integer> {
    private static Logger LOG = LoggerFactory.getLogger(CdfCli.class);

    @Override
    public Integer call() throws Exception {
        return 1;
    }

    /*
    The main entry point to the code. Activates the CLI, parses the input arguments and starts the execution method.
     */
    public static void main(String[] args) {
        try {
            int exitCode = new CommandLine(new CdfCli()).execute(args);
            if (exitCode != 0)
                throw new Exception("An error occurred during execution");
        } catch (Exception e) {
            LOG.error(e.toString());
            System.exit(-1);
        }
    }
}
