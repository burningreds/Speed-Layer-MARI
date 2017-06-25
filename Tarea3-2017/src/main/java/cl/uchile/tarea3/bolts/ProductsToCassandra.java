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
 * Inserts new product count into cassandra table (itemId, count)
 * @author Paula
 *
 */
public class ProductsToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ProductsToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long itemCount = 0;
        String itemId = tuple.getStringByField("item");
        System.out.println("Categoría: " + itemId);
        //Consultamos por el registro con la fecha solicitada
        ResultSet results = session.execute("SELECT * FROM product_count WHERE item_id ='" + itemId + "'");
        for (Row row : results) {
            itemCount = row.getLong("count") + 1;
        }

        Statement statement = QueryBuilder.insertInto("product_count")
        		.value("item_id", itemId)
        		.value("count", itemCount)
        		.value("update_date", LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()));
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
