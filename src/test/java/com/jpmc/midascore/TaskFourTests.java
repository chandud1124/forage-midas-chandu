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
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(5000);


        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");

        // Query wilbur's balance from database
        UserRecord wilbur = userRepository.findByName("wilbur");
        if (wilbur != null) {
            int wilburBalance = (int) wilbur.getBalance();
            logger.info("WILBUR'S BALANCE: {}", wilburBalance);
            logger.info("WILBUR'S BALANCE (float): {}", wilbur.getBalance());
            System.out.println("\n\n========== WILBUR'S BALANCE ==========");
            System.out.println("Balance (float): " + wilbur.getBalance());
            System.out.println("Balance (int): " + wilburBalance);
            System.out.println("=========================================\n\n");
        } else {
            logger.warn("Wilbur user not found");
        }
    }
}
