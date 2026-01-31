package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1)
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(5000);

        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");

        // Query waldorf's balance from database
        UserRecord waldorf = userRepository.findByName("waldorf");
        if (waldorf != null) {
            int waldorfBalance = (int) waldorf.getBalance();
            logger.info("WALDORF'S BALANCE: {}", waldorfBalance);
            logger.info("WALDORF'S BALANCE (float): {}", waldorf.getBalance());
            System.out.println("\n\n========== WALDORF'S BALANCE ==========");
            System.out.println("Balance (float): " + waldorf.getBalance());
            System.out.println("Balance (int): " + waldorfBalance);
            System.out.println("=========================================\n\n");
        } else {
            logger.warn("Waldorf user not found");
        }
    }
}
