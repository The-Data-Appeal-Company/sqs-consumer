# SQS-CONSUMER

[![license](https://img.shields.io/github/license/The-Data-Appeal-Company/spring-off.svg)](LICENSE.txt)

### Simple and concurrently consuming utility for AWS SQS
Sqs-consumer allows developers to consume messages from a sqs queue leveraging go's competition management. Use Sqs consumer is very simple, fast and clean.

### Usage

Sqs-consumer provides a simple configuration  *consumer.SQSConf* used by consumer and contains: the queue url, the consumer concurrency and the max number of messages that aws client can receive per request. 

```go
 confSQS := consumer.SQSConf{
        Queue:               "myQueueUrl",
        Concurrency:         1,
        MaxNumberOfMessages: 10,
    }
``` 

Further information about request limits can be retrieved in AWS official documentation: https://docs.aws.amazon.com/sdk-for-go/api/service/sqs/#ReceiveMessageInput

To consume messages from the queue with sqs-consumer you must provide a *consumer.ConsumerFn* that process your message and return, in case of fail an error. If *consumerFn* process a message without errors sqs-consumer will delete the message in the queue, otherwise message continue to live in the queue scope according to the queue definition. 
```go
 
cons, err := consumer.NewSQSConsumer(&confSQS, sqs.New(sess))

if err != nil {
    panic(err)
}

log.Infof("started %d worker on queue %s", confSQS.Concurrency, confSQS.Queue)

err = cons.Start(context.Background(), 
//consumer.ConsumerFn
func(data []byte) error {
    //do the dirty job here
    return nil // or error in case of fail
})

if err != nil {
    panic(err)
}
``` 


  