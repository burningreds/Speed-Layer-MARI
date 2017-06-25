/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.UUID;

/**
 * Q1 Top 10 Categories Query:
 * Selects top most purchased categories
 * @author Paula
 *
 */
public class TopCategories {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("tarea3");

        long count = 0;
        String category;
        
        ResultSet results = session.execute("SELECT * FROM top10categories");
        
        for (Row row : results) {
            category = row.getString("category");
            count = row.getLong("count");
            System.out.println(category + ": " + count);
        }
        
        cluster.close();
    }
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

}
