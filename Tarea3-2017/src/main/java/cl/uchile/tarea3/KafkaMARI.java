/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

/**
 *
 * @author Pipe
 */
public class KafkaMARI {

    public static void main(String[] argv) throws Exception {

        String topicName = "RetailMari";
        String groupId = "Grupo7";

        ConsumerThread consumerRunnable = new ConsumerThread(topicName, groupId);
        consumerRunnable.start();
        
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {

        private final String topicName;
        private final String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        @Override
        public void run() {
            //Propiedades de lectura de Retai MARI
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "50.116.6.150:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
            kafkaConsumer = new KafkaConsumer<>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            //Propiedades almacenamiento local de Kafka
            Properties configProperties_local = new Properties();
            configProperties_local.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties_local.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            configProperties_local.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            
            //Desde aqui se guarda en la cola local, OJO con las propiedades que se les pasa
            try (org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<>(configProperties_local)) {

                try {

                    while (true) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                        //Ciclo que consume de la cola del Retail MARI
                        for (ConsumerRecord<String, String> record : records) {
                            //obtenemos el registro de la cola
                            String json = record.value();

                            //imprimir el json en pantalla, nada especial
                            System.out.println(json);

                            //en estas dos lineas se guarda el string "json" en una cola del kafka local que se llama "kafka_local"
                            ProducerRecord<String, String> rec = new ProducerRecord<>("kafkaQ", json);
                            producer.send(rec);

                        }
                    }
                } catch (WakeupException ex) {
                    System.out.println("Exception caught " + ex.getMessage());
                } finally {
                    kafkaConsumer.close();
                    System.out.println("After closing KafkaConsumer");
                }
            }
            //aqui termina el código de la cola local
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

}
