# Cognite Data Fusion CLI

A CLI that enables you to work with Cognite Data Fusion resource types. 

The following resource types are supported:
- `Files`.

## Authentication

Currently, the CLI only supports authenticating with client credentials. There are two ways you can specify your credentials:
- By supplying a credentials file (json)
- Via parameters

### Via a credentials file

A convenient way of supplying credentials, is by referencing a credentials file. The file must be in `json` format and follow a schema:
```json
{
    "cdfHost" : "<your-cdf-host>", # optional parameter
    "cdfProject" : "<your-cdf-project>",
    "clientId" : "<your-client-id>",
    "clientSecret" : "<your-client-secret>",
    "aadTenantId" : "<your-add-tenant-id>"
}
```
You supply the file to the CLI via `--credentials-file[=<credentialsFile>]`.

### Via parameters

You can set the authentication credentials via individual parameters:
```text
      --client-id[=<clientId>]
                    The client ID to authenticate with.
      --client-secret[=<clientSecret>]
                    The client secret to authenticate with.
  -h, --cdf-host[=<cdfHost>]
                    The CDF base URL.
  -p, --cdf-project[=<cdfProject>]
                    The CDF project to work towards.
      --tenant-id[=<aadTenantId>]
```


## Working with CDF files

### Upload files to CDF

You can upload a single file or a directory to CDF. The files will get a basic header/metadata based on the file name.

The easiest way to run the CLI, is as a container via Docker (or similar):
```console
$ docker run -it -v c:\files:/files -v c:\creds:/creds kjetilh33/cdf-cli:latest files upload /files --credentials-file=/creds/creds.json
```
- `-it`: attaches the terminal to the container. Required for CLI input and output.
- `-v c:\files:/files`: Mounts the local directory hosting the files you want to upload to CDF.
- `-v c:\creds:/creds`: Mounts a local directory hosting the credentials file.
- `kjetilh33/cdf-cli:latest`: Use the latest build of the CLI.
- `files upload /files`: Upload files from the `/files` directory (mounted from `c:\files` locally).
- `--credentials-file=/creds/creds.json`: Specify the credentials file (mounted from `c:\creds` locally).

## Quickstart

You can run this module in several ways: 1) as a container, using Docker (recommended), 2) locally as a Java application.

### Run as a container using Docker

This is the recommended way of running the cli. You only need a container runtime, for example Docker Desktop.

You have to authenticate towards CDF with client credentials. You can pass the credentials to the CLI as single parameters or via a Json formatted `credentials file`

```console
$ docker run -it -v c:\files:/files -v c:\temp:/creds kjetilh33/cdf-cli:latest files upload --credentials-file=/creds/creds.json /files
```
- `-it`: attaches the terminal to the container.
- `-v`: mounts a local directory to the container. Use this to map the directory hosting the files to upload/download + the directory hosting the credentials file.

### Run as a local Java application

The minimum requirements for running the module locally:
- Java 17 SDK
- Maven

On Linux/MaxOS:
```console
$ mvn compile exec:java -Dexec.mainClass="com.cognite.cli.CdfCli"
```

On Windows Powershell:
```ps
> mvn compile exec:java -D exec.mainClass="com.cognite.cli.CdfCli"
```


