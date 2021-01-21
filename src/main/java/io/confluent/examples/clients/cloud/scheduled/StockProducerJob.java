package io.confluent.examples.clients.cloud.scheduled;

import com.google.gson.Gson;
import io.confluent.examples.clients.cloud.model.TickNewsTransformationDTO;
import io.polygon.kotlin.sdk.rest.PolygonRestClient;
import io.polygon.kotlin.sdk.rest.reference.PolygonReferenceClient;
import io.polygon.kotlin.sdk.rest.reference.TickerNewsDTO;
import io.polygon.kotlin.sdk.rest.reference.TickerNewsParameters;
import io.polygon.kotlin.sdk.rest.reference.TickerNewsParametersBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class StockProducerJob implements Job {

    public StockProducerJob() {}

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap data = jobExecutionContext.getMergedJobDataMap();
        String[] args = {data.getString("arg0"), data.getString("arg1")};
        try {
            produceFromPolygon(args);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public void produceFromPolygon(String[] args) throws IOException {
        StringBuilder fileContent = new StringBuilder();
        File file = new File("/home/alejo/Documentos/Development/University/SeminarioBd/KafkaExamples/clients/cloud/java/src/main/resources/availableEquities.json");
        Scanner reader = new Scanner(file);
        while (reader.hasNext()) fileContent.append(reader.nextLine());
        reader.close();

        Gson gson = new Gson();
        String[] symbolsArray = gson.fromJson(fileContent.toString(), String[].class);
        // Load properties from a local configuration file
        // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
        // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
        // Follow these instructions to create this file: https://docs.confluent.io/current/tutorials/examples/clients/docs/java.html
        final Properties props = loadConfig(args[0]);

        // Create topic if needed
        final String topic = args[1];
        createTopic(topic, props);

        // Add additional properties.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

        Producer<String, TickNewsTransformationDTO> producer = new KafkaProducer<>(props);

        //Consume Polygon data
        String polygonKey = System.getenv("POLYGON_API_KEY");
        PolygonRestClient client = new PolygonRestClient(polygonKey);
        PolygonReferenceClient referenceClient = client.getReferenceClient();
        TickerNewsParameters params = new TickerNewsParametersBuilder().symbol(symbolsArray[generateRandomNumber(0, symbolsArray.length)]).build();
        List<TickerNewsDTO> tickerNews = referenceClient.getTickerNewsBlocking(params);
        for (TickerNewsDTO tickerNew : tickerNews) {
            String key = tickerNew.getSymbols().get(0);
            producer.send(new ProducerRecord<>(topic, key, new TickNewsTransformationDTO(tickerNew)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                    }
                }
            });
        }
        producer.flush();
        System.out.printf("A lot of messages were produced to topic %s%n", topic);
        producer.close();
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public static int generateRandomNumber(int from, int to) {
        return (int) Math.round(Math.floor(Math.random() * (to - from) + from));
    }
}
