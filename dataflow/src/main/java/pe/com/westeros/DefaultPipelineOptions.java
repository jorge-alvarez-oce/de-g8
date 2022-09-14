package pe.com.westeros;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class DefaultPipelineOptions {

    public interface DatabaseToCloudStoragePipelineOptions extends PipelineOptions {

        @Description("Temporary directory for BigQuery loading process")
        @Default.String("gcp-data-engineer-project-001.access.payments")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

        @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
        @Default.String("com.mysql.cj.jdbc.Driver")
        ValueProvider<String> getDriverClassName();

        void setDriverClassName(ValueProvider<String> driverClassName);

        @Description("The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
        @Default.String("jdbc:mysql://34.125.166.191:3306/classicmodels")
        ValueProvider<String> getConnectionURL();

        void setConnectionURL(ValueProvider<String> connectionURL);

        @Description("JDBC connection user name. ")
        @Default.String("labfinalxs")
        ValueProvider<String> getUsername();

        void setUsername(ValueProvider<String> username);

        @Description("JDBC connection password. ")
        @Default.String("clase@patch")
        ValueProvider<String> getPassword();

        void setPassword(ValueProvider<String> password);

        @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
        @Default.String("select * from payments")
        ValueProvider<String> getQuery();

        void setQuery(ValueProvider<String> query);

        @Description("BigQuery Table spec to write the output to"
                + "for example: some-project-id:somedataset.sometable")
        ValueProvider<String> getOutputTable();

        void setOutputTable(ValueProvider<String> value);

        @Description("Output Cloud storage Lake. ")
        @Default.String("gs://my-bucket/input.csv")
        ValueProvider<String> getGcslake();

        void setGcslake(ValueProvider<String> gcinput);

        @Description("Csv delimiter ")
        @Default.String("customerNumber,checkNumber,paymentDate,amount")
        ValueProvider<String> getCsvHeaders();

        void setCsvHeaders(ValueProvider<String> csvDelimiter);

    }

    public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow() {
        return new ResultSetToTableRow();
    }

    private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

        static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        static DateTimeFormatter datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.SSSSSS");
        static SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSXXX");

        public TableRow mapRow(ResultSet resultSet) throws Exception {

            ResultSetMetaData metaData = resultSet.getMetaData();

            TableRow outputTableRow = new TableRow();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (resultSet.getObject(i) == null) {
                    outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
                    continue;
                }

                switch (metaData.getColumnTypeName(i).toLowerCase()) {
                    case "date":
                        outputTableRow.set(
                                metaData.getColumnName(i),
                                dateFormatter.format(resultSet.getDate(i).toLocalDate()));
                        break;
                    case "timestamp":
                        outputTableRow.set(
                                //metaData.getColumnName(i), dateFormatter.format(resultSet.getObject(i)));
                                metaData.getColumnName(i), timestampFormatter.format(resultSet.getObject(i)));
                        break;
                    case "datetime":
                        outputTableRow.set(
                                metaData.getColumnName(i),
                                datetimeFormatter.format((TemporalAccessor) resultSet.getObject(i)));
                        break;
                    default:
                        outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
                }

            }

            return outputTableRow;
        }
    }
}
