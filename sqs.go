package sqs_consumer

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"time"
)

const (
	DefaultMaxNumberOfMessages = 10
	DefaultVisibilityTimeout   = 20
	DefaultWaitTimeSeconds     = 5
	DefaultConcurrency         = 1
)

type SQSConf struct {
	Queue               string
	Concurrency         int
	MaxNumberOfMessages int64
	VisibilityTimeout   int64
	WaitTimeSeconds     int64
}

type SQS struct {
	config *SQSConf
	sqs    *sqs.SQS
}

func NewSQSWorker(conf *SQSConf, svc *sqs.SQS) (*SQS, error) {

	if conf.Queue == "" {
		return nil, errors.New("queue not set")
	}

	if conf.Concurrency == 0 {
		conf.Concurrency = DefaultConcurrency
	}

	if conf.WaitTimeSeconds == 0 {
		conf.WaitTimeSeconds = DefaultWaitTimeSeconds
	}

	if conf.VisibilityTimeout == 0 {
		conf.VisibilityTimeout = DefaultVisibilityTimeout
	}

	if conf.MaxNumberOfMessages == 0 {
		conf.MaxNumberOfMessages = DefaultMaxNumberOfMessages
	}

	return &SQS{config: conf, sqs: svc}, nil
}

func (s *SQS) Start(ctx context.Context, consumeFn ConsumerFn) error {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt)
		_ = <-c
		cancel()
	}()

	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < s.config.Concurrency; i++ {
		g.Go(func() error {
			return s.handleMessages(ctx, consumeFn)
		})
	}

	return g.Wait()
}

func (s *SQS) handleMessages(ctx context.Context, consumeFn ConsumerFn) error {
	receiveMessageRequest := &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &s.config.Queue,
		MaxNumberOfMessages: aws.Int64(s.config.MaxNumberOfMessages),
		VisibilityTimeout:   aws.Int64(s.config.VisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(s.config.WaitTimeSeconds),
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			result, err := s.sqs.ReceiveMessage(receiveMessageRequest)

			if err != nil {
				return err
			}

			if len(result.Messages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			toDelete := make([]*sqs.Message, 0)

			for _, msg := range result.Messages {
				if err := consumeFn([]byte(*msg.Body)); err != nil {
					logrus.Errorf("error %s", err.Error())
					continue
				}
				toDelete = append(toDelete, msg)
			}

			if err := s.deleteSqsMessages(toDelete); err != nil {
				return err
			}
		}
	}
}

func (s *SQS) deleteSqsMessages(msg []*sqs.Message) error {

	if len(msg) == 0 {
		return nil
	}

	batch := make([]*sqs.DeleteMessageBatchRequestEntry, len(msg))

	for i, v := range msg {
		batch[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            v.MessageId,
			ReceiptHandle: v.ReceiptHandle,
		}
	}

	_, err := s.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		Entries:  batch,
		QueueUrl: &s.config.Queue,
	})

	return err
}

