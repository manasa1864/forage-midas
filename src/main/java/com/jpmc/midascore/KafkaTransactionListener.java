package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final DatabaseConduit databaseConduit;
    private final IncentiveClient incentiveClient;

    public KafkaTransactionListener(UserRepository userRepository,
                                    DatabaseConduit databaseConduit,
                                    IncentiveClient incentiveClient) {

        this.userRepository = userRepository;
        this.databaseConduit = databaseConduit;
        this.incentiveClient = incentiveClient;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core"
    )
    public void receive(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId());

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = incentiveClient.query(transaction);

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive.getAmount());

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentive.getAmount());

        databaseConduit.saveTransaction(record);
    }
}