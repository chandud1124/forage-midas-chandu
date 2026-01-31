package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    private int count = 0;
    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public KafkaTransactionListener(DatabaseConduit databaseConduit, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        count++;
        System.out.println("DEBUG: Received transaction " + count + ": " + transaction);
        logger.info("===== Transaction #{} - AMOUNT: {} =====", count, transaction.getAmount());
        logger.info("Received transaction: {}", transaction);
        
        // Validate and process transaction
        processTransaction(transaction);
    }

    private void processTransaction(Transaction transaction) {
        // Get sender and recipient from database
        UserRecord sender = databaseConduit.getUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.getUserById(transaction.getRecipientId());
        
        logger.info("Looking for sender ID: {}, Found: {}", transaction.getSenderId(), sender != null ? sender.getName() : "null");
        logger.info("Looking for recipient ID: {}, Found: {}", transaction.getRecipientId(), recipient != null ? recipient.getName() : "null");
        
        // Validate transaction
        if (sender == null) {
            logger.warn("Invalid senderId: {}", transaction.getSenderId());
            return;
        }
        
        if (recipient == null) {
            logger.warn("Invalid recipientId: {}", transaction.getRecipientId());
            return;
        }
        
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender: {} (has {}, needs {})", transaction.getSenderId(), sender.getBalance(), transaction.getAmount());
            return;
        }
        
        // Transaction is valid - get incentive from API
        float incentive = 0.0f;
        try {
            Incentive incentiveResponse = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            if (incentiveResponse != null) {
                incentive = incentiveResponse.getAmount();
                logger.info("Received incentive: {} for transaction", incentive);
            }
        } catch (Exception e) {
            logger.warn("Failed to get incentive from API: {}", e.getMessage());
        }
        
        // Update balances
        // Sender loses the transaction amount (not the incentive)
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        // Recipient gains the transaction amount AND the incentive
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive);
        
        // Update user balances in database
        databaseConduit.updateUserBalance(sender);
        databaseConduit.updateUserBalance(recipient);
        
        // Save transaction record with incentive
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive);
        databaseConduit.saveTransaction(transactionRecord);
        
        logger.info("Transaction processed successfully. {} balance now: {}, {} balance now: {} (incentive: {})", 
            sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance(), incentive);
    }
}
