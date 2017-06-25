/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.Date;
import java.util.UUID;

/**
 *
 * @author FelipeEsteban
 */
public class Cassandra {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("mari");

        double probabilidad;
        LocalDate date;
        ResultSet results = session.execute("SELECT * FROM probabilidad");
        for (Row row : results) {
            probabilidad = (double)row.getLong("entrega_rut") / (double)row.getLong("total_rut");
            date = row.getDate("date");
            System.out.println("Date: " + date + "Probabilidad: " + probabilidad);
        }
        cluster.close();
    }
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    public static long getTimeFromUUID(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

}
