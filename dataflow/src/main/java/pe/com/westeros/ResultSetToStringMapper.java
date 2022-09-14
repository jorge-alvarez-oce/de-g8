package pe.com.westeros;

import lombok.AllArgsConstructor;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

@AllArgsConstructor(staticName = "newInstance")
public class ResultSetToStringMapper implements JdbcIO.RowMapper<String> {

    @Override
    public String mapRow(ResultSet resultSet) throws Exception {
        ResultSetMetaData metaData = resultSet.getMetaData();
        StringBuilder builder = new StringBuilder();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {


            builder.append(resultSet.getObject(i).toString()
                    .replaceAll("[\\t\\n\\r]+", " ")
                    .replaceAll(";", ","));

            if (i != metaData.getColumnCount()) {
                builder.append(";");
            }
        }

        return builder.toString();
    }
}