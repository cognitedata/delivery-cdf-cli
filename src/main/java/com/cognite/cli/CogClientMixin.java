package com.cognite.cli;

import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Shared utility class offering access to the {@link CogniteClient} and helper methods.
 */
public class CogClientMixin {
    private static Logger LOG = LoggerFactory.getLogger(CogClientMixin.class);
    
    // global data structures
    private static CogniteClient cogniteClient;
    private static JsonNode credentialsFileRootNode;
    private static ObjectMapper objectMapper = new ObjectMapper();

    @Option(names = {"-p", "--cdf-project"}, description = "The CDF project to authenticate to.", arity = "0..1",
            interactive = true, echo = true)
    private String cdfProject;

    @Option(names = {"-h", "--cdf-host"}, description = "The CDF base URL.", arity = "0..1", interactive = true,
            echo = true, defaultValue = "https://api.cognitedata.com")
    private String cdfHost;

    @Option(names = "--client-id", description = "The client ID to authenticate with.", arity = "0..1", interactive = true)
    private String clientId;

    @Option(names = "--client-secret", description = "The client secret to authenticate with.", arity = "0..1", interactive = true)
    private String clientSecret;

    @Option(names = "--tenant-id", description = "The AAD tenant ID to authenticate towards.", arity = "0..1", interactive = true)
    private String aadTenantId;

    @Option(names = "--credentials-file", description = "A file hosting CDF credentials.", arity = "0..1", interactive = true)
    private Path credentialsFile;

    public void verify() {
        // Check all mandatory parameters
        // If a credentials file is specified, then we don't need to check the individual parameters
        if (null != credentialsFile)
            return;

        // No credentials file is specified. Check individual parameters.
        if (null == cdfProject && null != System.console())
            cdfProject = System.console().readLine("Enter value for --cdf-project: ");
        if (null == clientId && null != System.console())
            clientId = System.console().readPassword("Enter value for --client-id: ").toString();
        if (null == clientSecret && null != System.console())
            clientSecret = System.console().readPassword("Enter value for --client-secret: ").toString();
        if (null == aadTenantId && null != System.console())
            aadTenantId = System.console().readPassword("Enter value for --tenant-id: ").toString();
    }

    /**
    Return the Cognite client.

    If the client isn't instantiated, it will be created according to the configured authentication options. After the
    initial instantiation, the client will be cached and reused.
     */
    public CogniteClient getCogniteClient() throws Exception {
        if (null == cogniteClient) {
            // The client has not been instantiated yet
            verify();

            ClientConfig clientConfig = ClientConfig.create()
                    .withUpsertMode(UpsertMode.REPLACE);

            cogniteClient = CogniteClient.ofClientCredentials(
                            getClientId(),
                            getClientSecret(),
                            TokenUrl.generateAzureAdURL(getAadTenantId()))
                    .withProject(getCdfProject())
                    .withBaseUrl(getCdfHost())
                    .withClientConfig(clientConfig);
        }

        return cogniteClient;
    }

    private String getCdfHost() throws Exception {
        if (null != credentialsFile && getCredentialsFileRootNode().path("cdfHost").isTextual()) {
            return getCredentialsFileRootNode().path("cdfHost").textValue();
        } else {
            return cdfHost;
        }
    }

    private String getCdfProject() throws Exception {
        if (null != cdfProject) {
            return cdfProject;
        } else {
            return getCredentialsFileRootNode().path("cdfProject").textValue();
        }
    }

    private String getClientId() throws Exception {
        if (null != clientId) {
            return clientId;
        } else {
            return getCredentialsFileRootNode().path("clientId").textValue();
        }
    }

    private String getClientSecret() throws Exception {
        if (null != clientSecret) {
            return clientSecret;
        } else {
            return getCredentialsFileRootNode().path("clientSecret").textValue();
        }
    }

    private String getAadTenantId() throws Exception {
        if (null != aadTenantId) {
            return aadTenantId;
        } else {
            return getCredentialsFileRootNode().path("aadTenantId").textValue();
        }
    }

    private JsonNode getCredentialsFileRootNode() throws Exception {
        if (null == credentialsFileRootNode) {
            if (null == credentialsFile) {
                String message = "Trying to parse credentials file, but the file is not specified.";
                LOG.error(message);
                throw new IOException(message);
            }
            String jsonFileString = Files.readString(credentialsFile);
            credentialsFileRootNode = objectMapper.readTree(jsonFileString);
        }

        return credentialsFileRootNode;
    }
}
