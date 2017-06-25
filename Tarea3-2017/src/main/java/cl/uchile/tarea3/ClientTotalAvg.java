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
 * Q2 Avg by client Query:
 * Selects avg amount spent per purchase by each client 
 * @author Paula
 *
 */
public class ClientTotalAvg {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("tarea3");

        double avg = 0;
        
        ResultSet results = session.execute("SELECT * FROM client_total");
        
        for (Row row : results) {
            avg = (double)row.getDouble("total") / (double)row.getLong("count");
            System.out.println(row.getString("client") + ": " + avg);
        }
        
        cluster.close();
    }
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

}
