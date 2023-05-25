<?php

declare(strict_types=1);

namespace Kafka;

use Exception;
use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected \RdKafka\KafkaConsumer $consumer;

    protected $producer;

    public function __construct($producer, \RdKafka\KafkaConsumer $consumer )
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }


    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        print "pushed";
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(2000);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    /**
     * @throws Exception
     */
    public function pop($queue = null)
    {
        try {
            $this->consumer->subscribe([$queue ?? env('KAFKA_QUEUE')]);
            $message = $this->consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages; will wait for more");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("Timed out");
                    break;
                default:
                    throw new Exception($message->errstr(), $message->err);
            }
        } catch (Exception $e) {
            var_dump($e->getMessage());
        }
    }
}
