import java.util.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.Duration;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.transforms.Flatten;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.KV;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.Compression;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.FILE_LOADS;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;

import org.apache.beam.sdk.coders.StringUtf8Coder;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author user
 */
public class PubSubToBigQueryPipeline
{
    private static final Logger logger = LoggerFactory.getLogger(PubSubToBigQueryPipeline.class);
    public static void main(String[] args) throws Exception {
        String projectId = System.getProperty("projectId");
        String workingBucket = System.getProperty("workingBucket");
        String serviceAccount = System.getProperty("serviceAccount");
        String pipelineName = System.getProperty("pipeline");
        String dataset = System.getProperty("dataset");
        String table = System.getProperty("table");
        boolean streaming = Boolean.valueOf(System.getProperty("streaming"));
        int maxWorker = Integer.parseInt(System.getProperty("maxWorker"));
        String machineType = System.getProperty("machineType");
        String topic = System.getProperty("topic");
        String keyFile = System.getProperty("keyFile");
        String region = System.getProperty("region");
        String subnetwork = System.getProperty("subnetwork");
        String stagingBucket = "gs://" + workingBucket + "/staging";
        String tempBucket = "gs://" + workingBucket + "/temp";
        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
        GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(keyFile)).createScoped(Arrays.asList(new String[] {
                "https://www.googleapis.com/auth/cloud-platform" }));
        options.setRunner(DataflowRunner.class);
        options.setProject(projectId);
        options.setStagingLocation(stagingBucket);
        options.setTempLocation(tempBucket);
        options.setGcpTempLocation(tempBucket);
        options.setGcpCredential(credential);
        options.setServiceAccount(serviceAccount);
        options.setMaxNumWorkers(maxWorker);
        options.setWorkerMachineType(machineType);
        options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
        options.setRegion(region);
        options.setStreaming(streaming);
        options.setJobName(pipelineName);
        options.setUsePublicIps(false);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(PubsubIO.readStrings().fromTopic(topic)).apply("ConvertDataToTableRows", ParDo.of(new DoFn<String, TableRow>() {
            private static final long serialVersionUID = 1L;
            @ProcessElement
            public void processElement(ProcessContext pc) {
                logger.info("Converting pubsub message to tablerow");
                String messagePayload = pc.element();
                Map<String, String> message = new Gson().fromJson(messagePayload, new TypeToken<HashMap<String, String>>() {
                }.getType());
                String id = message.get("id");
                String name= message.get("name");
                String surname=message.get("surname");
                logger.debug("Creating table row");
                logger.debug("id : " + id + " :: " + "name : " + name+ " :: " +"surname : " + surname );
                TableRow row = new TableRow().set("id", id).set("name", name).set("surname", surname);
                c.output(row);
            }
        })).apply("InsertTableRowsToBigQuery", BigQueryIO.writeTableRows().to(projectId + ":" + dataset + "."
                + table).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
// Run the pipeline
        pipeline.run();
    }
}
