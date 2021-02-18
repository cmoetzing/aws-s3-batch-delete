package net.moetzing.tools;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;

public class App {
    // required
    private static final Option regionOption = Option.builder().argName("region").longOpt("region").desc("the AWS region").hasArg().required().build();
    private static final Option bucketOption = Option.builder().argName("bucket").longOpt("bucket").desc("the AWS bucket").hasArg().required().build();
    private static final Option profileOption = Option.builder().argName("profile").longOpt("profile").desc("the AWS credentials profile").hasArg().required().build();

    // optional
    private static final Option serviceEndpoint = Option.builder().argName("serviceEndpoint").longOpt("serviceEndpoint").desc("the AWS service endpoint region").hasArg().optionalArg(true).build();
    private static final Option threadsOption = Option.builder().argName("threads").longOpt("threads").type(Integer.class).desc("the number of parallel requests").hasArg().optionalArg(true).build();
    private static final Option prefixOption = Option.builder().argName("prefix").longOpt("prefix").desc("the AWS prefix").hasArg().optionalArg(true).build();

    public static final String DEFAULT_THREADS = "4";

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(regionOption);
        options.addOption(bucketOption);
        options.addOption(profileOption);
        options.addOption(prefixOption);
        options.addOption(threadsOption);
        options.addOption(serviceEndpoint);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            new App().run(commandLine);
        } catch(MissingOptionException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "aws-s3-batch-delete.jar",  options, true);
            System.exit(1);
        }
    }

    private void run(CommandLine commandLine) {
        String region = commandLine.getOptionValue(regionOption.getArgName());
        String bucketName = commandLine.getOptionValue(bucketOption.getArgName());
        String profile = commandLine.getOptionValue(profileOption.getArgName());
        String prefix = commandLine.getOptionValue(prefixOption.getArgName(), null);

        String tmp = commandLine.getOptionValue(threadsOption.getArgName(), DEFAULT_THREADS);
        if(!NumberUtils.isCreatable(tmp)) {
            System.err.println("error: argument " + threadsOption.getArgName() + " with value "+  tmp + " can not be parsed to a number");
            System.exit(2);
        }
        Integer threads = NumberUtils.createInteger(tmp);

        printInfoWithCountDown(commandLine, region, bucketName, profile, prefix, threads);

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threads, threads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new AbortPolicy());
        try {
            AmazonS3 s3Client;
            if(commandLine.hasOption(serviceEndpoint.getArgName())) {
                String endpoint = commandLine.getOptionValue(serviceEndpoint.getArgName());
                EndpointConfiguration conf = new EndpointConfiguration(endpoint, region);
                s3Client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new ProfileCredentialsProvider(profile))
                        .withEndpointConfiguration(conf)
                        .build();
            } else {
                s3Client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new ProfileCredentialsProvider(profile))
                        .withRegion(region)
                        .build();
            }

            ObjectListing objectListing = s3Client.listObjects(bucketName, prefix);
            while (true) {
                Runnable runnable = new Delete(s3Client, bucketName, objectListing);

                boolean notRunning = true;
                while(notRunning) {
                    try {
                        threadPoolExecutor.execute(runnable);
                        notRunning = false;
                    } catch (RejectedExecutionException e) {
                        System.out.println("waiting for next execution slot");
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException interruptedException) {
                            // suppress
                        }
                    }
                }

                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }

            threadPoolExecutor.shutdown();
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }

    private void printInfoWithCountDown(CommandLine commandLine, String clientRegion, String bucketName, String profile, String prefix, Integer threads) {
        if(commandLine.hasOption(prefixOption.getArgName())) {
            System.out.println("Delete all files from bucket '" + bucketName + "' in region '" + clientRegion + "' under prefix '" + prefix + "' logged in with profile '" + profile + "'." );
        } else {
            System.out.println("Delete all files from bucket '" + bucketName + "' in region '" + clientRegion + "' logged in with profile '" + profile + "'." );
        }

        System.out.println("Using " + threads + " threads.");
        System.out.print("Starting in ");
        for(int i=10; i>0; i--) {
            System.out.print(Integer.valueOf(i) + " ");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // suppress
            }
        }
        System.out.println("now");
    }

    private static class Delete implements Runnable {
        private AmazonS3 client;
        private String bucketName;
        private ObjectListing objectListing;

        private Delete(AmazonS3 client, String bucketName, ObjectListing objectListing) {
            this.client = client;
            this.bucketName = bucketName;
            this.objectListing = objectListing;
        }

        @Override
        public void run() {
            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();

            Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
            while (objIter.hasNext()) {
                String key = objIter.next().getKey();
                keys.add(new DeleteObjectsRequest.KeyVersion(key));
            }

            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName);
            request.withKeys(keys);
            try {
                DeleteObjectsResult deleteObjectsResult = client.deleteObjects(request);
                System.out.println("deleted next " + deleteObjectsResult.getDeletedObjects().size() + " after " + deleteObjectsResult.getDeletedObjects().get(0).getKey());
            } catch (MultiObjectDeleteException exception) {
                List<DeleteObjectsResult.DeletedObject> deletedObjects = exception.getDeletedObjects();
                if(deletedObjects != null && !deletedObjects.isEmpty()) {
                    System.out.println("deleted next " + deletedObjects.size() + " after " + deletedObjects.get(0).getKey());
                }

                if(!exception.getErrors().isEmpty()) {
                    String msg = exception.getErrors().stream().map(error -> "\t" + error.getKey() + ": " + error.getMessage()).reduce("", (accumulated, element) -> accumulated + "\n" + element);
                    System.err.println("Could not delete following objects:\n" + msg);
                }
            }
        }
    }
}
