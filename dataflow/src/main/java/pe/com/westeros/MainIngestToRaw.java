package pe.com.westeros;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.TimeZone;

public class MainIngestToRaw {

    public static void main(String[] args) {
        TimeZone timeZone = TimeZone.getTimeZone("America/Lima");
        TimeZone.setDefault(timeZone);
        System.setProperty("user.timezone", "America/Lima");

        DefaultPipelineOptions.DatabaseToCloudStoragePipelineOptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args)
                        .as(DefaultPipelineOptions.DatabaseToCloudStoragePipelineOptions.class);

        run(pipelineOptions);
    }

    private static PipelineResult run(DefaultPipelineOptions.DatabaseToCloudStoragePipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Extraccion de JDBC",
                        JdbcIO.<String>read()
                                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                                options.getDriverClassName(),
                                                options.getConnectionURL()
                                        )
                                        .withUsername(options.getUsername())
                                        .withPassword(options.getPassword()))
                                .withQuery(options.getQuery())
                                .withRowMapper(ResultSetToStringMapper.newInstance())
                )
                .setCoder(StringUtf8Coder.of())
                .apply("Proceso Cloud storage",
                        ParDo.of(new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                c.output(c.element().toString());
                            }
                        }))
                .apply("Deposito en GCS",
                        TextIO.write()
                                .to(options.getGcslake())
                                .withSuffix("csv")
                                .withoutSharding()
                );
        return pipeline.run();
    }
}
