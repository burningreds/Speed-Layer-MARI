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
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates top10 products table (productid, productcount, updatedate)
 * @author Paula
 *
 */
public class TopProductsToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TopProductsToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String itemId = tuple.getStringByField("item");
        String count = tuple.getStringByField("count");
        System.out.println("Categoría: " + itemId + count);
        Statement statement = QueryBuilder.insertInto("top10products")
        		.value("item_id", itemId)
        		.value("count", count)
        		.value("update_date", LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()));
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
