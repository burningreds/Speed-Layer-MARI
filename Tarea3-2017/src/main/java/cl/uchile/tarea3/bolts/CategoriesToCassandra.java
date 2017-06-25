/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.bolts;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts new category count into cassandra table (categoryname, categorycount)
 * The idea is to update the previous data with the new count (i think)
 * @author Paula
 *
 */
public class CategoriesToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CategoriesToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long categoryCount = 0;
        String categoryName = tuple.getStringByField("category");
        System.out.println("Categoría: " + categoryName);
        //Consultamos por el registro con la fecha solicitada
        ResultSet results = session.execute("SELECT * FROM category_count WHERE category ='" + categoryName + "'");
        for (Row row : results) {
            categoryCount = row.getLong("count") + 1;
        }

        Statement statement = QueryBuilder.insertInto("category_count")
        		.value("category", categoryName)
        		.value("count", categoryCount)
        		.value("update_date", LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()));
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	;
    }

}
