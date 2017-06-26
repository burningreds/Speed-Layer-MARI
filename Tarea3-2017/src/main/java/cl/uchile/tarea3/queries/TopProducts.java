/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.queries;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.UUID;

/**
 * Q3 Top 10 Products Query:
 * Selects top most purchased products
 * @author Paula
 *
 */
public class TopProducts {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("tarea3");
        
        int itemId = 0;
        long count = 0;
        String name;
        
        ResultSet results = session.execute("SELECT * FROM top10products");
        
        for (Row row : results) {
            itemId = row.getInt("item_id");
            count = row.getLong("count");
            name = row.getString("name");
            System.out.println(itemId + " " + name + ": " + count);
        }
        
        cluster.close();
    }
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

}
