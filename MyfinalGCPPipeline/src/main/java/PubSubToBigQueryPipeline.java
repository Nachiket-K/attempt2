import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.JsonToRow;

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
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQueryPipeline.class);

    public interface PipelineOptions extends DataflowPipelineOptions {
        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("BigQuery table name")
        String getTableName();
        void setTableName(String tableName);

        @Description("input Subscription of PubSub")
        String getSubscription();
        void setSubscription(String subscription);

        @Description("DLQ topic of PubSub")
        String getDlqTopic();
        void setDlqTopic(String dlqTopic);
    }

    public static void main(String[] args)
    {
        PipelineOptionsFactory.register(PipelineOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        run(options);
    }
    public static final Schema RSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();
    public static PipelineResult run(PipelineOptions options) 
    {
        String PSsubscriptionName="projects/"+options.getProject()+"/subscriptions/"+options.getSubscription();
        String oTableName=options.getProject()+":"+options.getTableName();
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName(options.getJobName());
        PCollection<String> DataOne=pipeline.apply("ReadMessageFromPubSub", PubsubIO.readStrings().fromSubscription(PSsubscriptionName));

        DataOne.apply("TransformToRow", JsonToRow.withSchema(RSchema))
                .apply("WriteDataToTable",
                        BigQueryIO.<Row>write().to(oTableName).useBeamSchema()
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        return pipeline.run();
       
    }
}
