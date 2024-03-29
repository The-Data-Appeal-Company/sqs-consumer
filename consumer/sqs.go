package consumer

import (
	"context"
	"errors"
	"github.com/The-Data-Appeal-Company/batcher-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"time"
)

var (
	DefaultMaxNumberOfMessages = int64(10)
	DefaultWaitTimeSeconds     = int64(5)
	DefaultConcurrency         = 1
	DefaultDeleteStrategy      = DeleteStrategyOnSuccess
)

type DeleteStrategy string

var (
	DeleteStrategyImmediate = DeleteStrategy("IMMEDIATE")
	DeleteStrategyOnSuccess = DeleteStrategy("ON_SUCCESS")
)

type SQSConf struct {
	Queue               string
	Concurrency         int
	MaxNumberOfMessages int64
	VisibilityTimeout   int64
	WaitTimeSeconds     int64
	DeleteStrategy      DeleteStrategy
}

type SQS struct {
	config *SQSConf
	sqs    sqsiface.SQSAPI
}

func NewSQSConsumer(conf *SQSConf, svc sqsiface.SQSAPI) (*SQS, error) {

	if conf.Queue == "" {
		return nil, errors.New("queue not set")
	}

	if conf.Concurrency == 0 {
		conf.Concurrency = DefaultConcurrency
	}

	if conf.WaitTimeSeconds == 0 {
		conf.WaitTimeSeconds = DefaultWaitTimeSeconds
	}

	if conf.MaxNumberOfMessages == 0 {
		conf.MaxNumberOfMessages = DefaultMaxNumberOfMessages
	}

	if len(conf.DeleteStrategy) == 0 {
		conf.DeleteStrategy = DefaultDeleteStrategy
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
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			result, err := s.sqs.ReceiveMessage(s.pullMessagesRequest())

			if err != nil {
				return err
			}

			if len(result.Messages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			if s.config.DeleteStrategy == DeleteStrategyImmediate {
				if err := s.deleteSqsMessages(result.Messages); err != nil {
					return err
				}
			}

			toDelete := make([]*sqs.Message, 0)
			for _, msg := range result.Messages {
				if err := consumeFn([]byte(*msg.Body)); err != nil {
					logrus.Errorf("error %s", err.Error())
					continue
				}

				if s.config.DeleteStrategy == DeleteStrategyOnSuccess {
					toDelete = append(toDelete, msg)
				}
			}

			if err := s.deleteSqsMessages(toDelete); err != nil {
				return err
			}

		}
	}
}

func (s *SQS) StartBatched(ctx context.Context, batcher *batcher.Batcher, consumeFn ConsumerBatchFn) error {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt)
		_ = <-c
		cancel()
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				result, err := s.sqs.ReceiveMessage(s.pullMessagesRequest())

				if err != nil {
					panic(err)
				}

				if len(result.Messages) == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				for _, msg := range result.Messages {
					batcher.Accumulate(msg)
				}

			}
		}

	}()

	return batcher.Start(ctx, func(batch []interface{}) error {
		msgBatch := make([]*sqs.Message, len(batch))
		dataBatch := make([][]byte, len(batch))

		for i := range batch {
			msgBatch[i] = batch[i].(*sqs.Message)
			dataBatch[i] = []byte(*batch[i].(*sqs.Message).Body)
		}

		if s.config.DeleteStrategy == DeleteStrategyImmediate {
			if err := s.deleteSqsMessages(msgBatch); err != nil {
				return err
			}
		}

		err := consumeFn(dataBatch)
		if err != nil {
			logrus.Error("error processing batch: ", err)
			return nil
		}

		if s.config.DeleteStrategy == DeleteStrategyOnSuccess {
			if err := s.deleteSqsMessages(msgBatch); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *SQS) handleMessagesBatched(ctx context.Context, batch *batcher.Batcher) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			result, err := s.sqs.ReceiveMessage(s.pullMessagesRequest())

			if err != nil {
				return err
			}

			if len(result.Messages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range result.Messages {
				batch.Accumulate(msg)
			}

		}
	}
}

func (s *SQS) pullMessagesRequest() *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &s.config.Queue,
		MaxNumberOfMessages: aws.Int64(s.config.MaxNumberOfMessages),
		VisibilityTimeout:   s.getVisibilityTimeout(),
		WaitTimeSeconds:     aws.Int64(s.config.WaitTimeSeconds),
	}
}

func (s *SQS) getVisibilityTimeout() *int64 {
	visibilityTimeout := aws.Int64(s.config.VisibilityTimeout)

	if s.config.VisibilityTimeout == 0 {
		visibilityTimeout = nil
	}

	return visibilityTimeout
}

func (s *SQS) deleteSqsMessages(msg []*sqs.Message) error {
	if len(msg) == 0 {
		return nil
	}

	chunks := chunk(msg, 10) //max batch size for sqs is 10

	for _, chunk := range chunks {
		batch := make([]*sqs.DeleteMessageBatchRequestEntry, len(chunk))

		for i, v := range chunk {
			batch[i] = &sqs.DeleteMessageBatchRequestEntry{
				Id:            v.MessageId,
				ReceiptHandle: v.ReceiptHandle,
			}
		}

		_, err := s.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
			Entries:  batch,
			QueueUrl: &s.config.Queue,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func chunk(rows []*sqs.Message, chunkSize int) [][]*sqs.Message {
	var chunk []*sqs.Message
	chunks := make([][]*sqs.Message, 0, len(rows)/chunkSize+1)

	for len(rows) >= chunkSize {
		chunk, rows = rows[:chunkSize], rows[chunkSize:]
		chunks = append(chunks, chunk)
	}

	if len(rows) > 0 {
		chunks = append(chunks, rows[:len(rows)])
	}

	return chunks
}
