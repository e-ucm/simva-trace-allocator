import { Kafka, logLevel } from 'kafkajs';
import { logger } from "./logger.js";

/**
 * @typedef KafkaOpts
 * @property {string} clientId
 * @property {string[]} brokers
 * @property {string} groupId
 * @property {string} topic
 */

export class KafkaClient {
    constructor({ clientId, brokers, groupId, topic }) {
        this.clientId = clientId;
        this.brokers = brokers;
        this.groupId = groupId;
        this.topic = topic;
        this.kafka = new Kafka({ logLevel: logLevel.INFO, clientId: this.clientId, brokers: this.brokers });
        this.consumer = this.kafka.consumer({ groupId: this.groupId });
    }

    // Connect and subscribe to a topic
    async connect() {
        try {
            logger.info(`Connecting to Kafka brokers: ${this.brokers}`);
            await this.consumer.connect();
            await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });
            logger.info(`Subscribed to topic: ${this.topic}`);
        } catch (error) {
            console.error('Error connecting to Kafka:', error);
        }
    }

    // Run the consumer and process messages
    async consumeLatestMessages(onMessage) {
        try {
            await this.connect();

            logger.info(`Listening for messages from topic: ${this.topic}`);

            await this.consumer.run({
                // By default, eachMessage is invoked sequentially for each message in each partition. 
                // In order to concurrently process several messages per once, you can increase the partitionsConsumedConcurrently option.
                partitionsConsumedConcurrently: 1, 
                eachMessage: async ({ topic, partition, message }) => {
                    const msgValue = message.value.toString();
                    const msgOffset = message.offset;
                    const msgPartition = partition;

                    const messageInfo = {
                        topic,
                        partition: msgPartition,
                        offset: msgOffset,
                        value: msgValue
                    };

                    // Call the provided onMessage callback with the message info
                    onMessage(messageInfo);
                }
            });
        } catch (error) {
            console.error('Error consuming messages:', error);
        }
    }

    // Disconnect the consumer
    async disconnect() {
        try {
            await this.consumer.disconnect();
            logger.info('Kafka consumer disconnected');
        } catch (error) {
            console.error('Error disconnecting from Kafka:', error);
        }
    }
}
