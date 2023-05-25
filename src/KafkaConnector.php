<?php

declare(strict_types=1);

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

/**
 * Class KafkaConnector
 * @package App\Connector
 */
class KafkaConnector implements ConnectorInterface
{

    /**
     * @param array $config
     * @return KafkaQueue
     */
    public function connect(array $config): KafkaQueue
    {
        $conf = new \RdKafka\Conf();
        $conf->set('bootstrap.servers', $config['boostrap_servers']);
        $conf->set('security.protocol', $config['security_protocol']);
        $conf->set('sasl.mechanism', $config['sasl_mechanism']);
        $conf->set('sasl.username', $config['sasl_username']);
        $conf->set('sasl.password', $config['sasl_password']);

        $producer = new \RdKafka\Producer($conf);

        $conf->set('group.id', $config['group_id']);
        $conf->set('auto.offset.reset', 'earliest');
        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
