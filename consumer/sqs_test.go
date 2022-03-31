package consumer

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/client/metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type SqsMock struct {
	sqsiface.SQSAPI
	inputs       []*sqs.ReceiveMessageInput
	receiveError error
	deleteInputs []*sqs.DeleteMessageBatchInput
	deleteError  error
}

func NewSqsMock(receiveError error, deleteError error) *SqsMock {
	return &SqsMock{inputs: make([]*sqs.ReceiveMessageInput, 0), receiveError: receiveError, deleteInputs: make([]*sqs.DeleteMessageBatchInput, 0), deleteError: deleteError}
}

func (s *SqsMock) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	if s.receiveError != nil {
		return nil, s.receiveError
	}
	s.inputs = append(s.inputs, input)
	return getQueueContent(), nil
}

func (s *SqsMock) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	if s.deleteError != nil {
		return nil, s.deleteError
	}
	s.deleteInputs = append(s.deleteInputs, input)
	return nil, nil
}

func TestNewSQSWorker(t *testing.T) {
	sqsConf := &SQSConf{
		Queue:               "queue",
		Concurrency:         2,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20,
	}

	svc := &sqs.SQS{
		Client: &client.Client{
			Retryer:    nil,
			ClientInfo: metadata.ClientInfo{},
			Config:     aws.Config{},
			Handlers:   request.Handlers{},
		},
	}

	type args struct {
		conf *SQSConf
		svc  sqsiface.SQSAPI
	}
	tests := []struct {
		name    string
		args    args
		want    *SQS
		wantErr bool
	}{
		{
			name: "shouldCreateNewSQSConsumer",
			args: args{
				conf: sqsConf,
				svc:  svc,
			},
			want: &SQS{
				config: sqsConf,
				sqs:    svc,
			},

			wantErr: false,
		},
		{
			name: "shouldCreateNewSQSConsumerWithDefaultValues",
			args: args{
				conf: &SQSConf{
					Queue: "queue",
				},
				svc: svc,
			},
			want: &SQS{
				config: &SQSConf{
					Queue:               "queue",
					Concurrency:         DefaultConcurrency,
					MaxNumberOfMessages: DefaultMaxNumberOfMessages,
					WaitTimeSeconds:     DefaultWaitTimeSeconds,
					DeleteStrategy:      DefaultDeleteStrategy,
				},
				sqs: svc,
			},

			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSQSConsumer(tt.args.conf, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSQSConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSQS_handleMessages(t *testing.T) {
	queueUrl := "queue"

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)

	var actual []string

	type fields struct {
		config *SQSConf
		sqs    sqsiface.SQSAPI
	}
	type args struct {
		ctx       context.Context
		consumeFn ConsumerFn
	}
	var tests = []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "shouldHandleMessage",
			fields: fields{
				config: &SQSConf{
					Queue: queueUrl,
				},
				sqs: NewSqsMock(nil, nil),
			},
			args: args{
				ctx: ctx,
				consumeFn: func(data []byte) error {
					actual = append(actual, string(data))
					return nil
				},
			},
			wantErr: false,
		},
		{
			name: "shouldHandleMessageWithError",
			fields: fields{
				config: &SQSConf{
					Queue:             queueUrl,
					VisibilityTimeout: 0,
				},
				sqs: NewSqsMock(nil, nil),
			},
			args: args{
				ctx: ctx,
				consumeFn: func(data []byte) error {
					return fmt.Errorf("error consume for message %s", string(data))
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		actual = make([]string, 0)
		t.Run(tt.name, func(t *testing.T) {
			s, _ := NewSQSConsumer(tt.fields.config, tt.fields.sqs)
			if err := s.handleMessages(tt.args.ctx, tt.args.consumeFn); err != nil {
				t.Errorf("handleMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			mock := tt.fields.sqs.(*SqsMock)
			if !tt.wantErr {
				require.NotNil(t, mock.inputs)
				require.NotNil(t, mock.deleteInputs)
				for _, msg := range actual {
					assert.Contains(t, []string{
						"msg1",
						"msg2",
						"msg3",
					}, msg)
				}
			} else {
				require.Len(t, mock.deleteInputs, 0)
				require.Len(t, actual, 0)
			}
		})
	}
}

func TestSQS_getVisibilityTimeout(t *testing.T) {
	type fields struct {
		config *SQSConf
		sqs    sqsiface.SQSAPI
	}
	tests := []struct {
		name   string
		fields fields
		want   *int64
	}{
		{
			name: "shouldGetVisibilityTimeoutFromConfig",
			fields: fields{
				config: &SQSConf{
					Queue:             "queue-test",
					VisibilityTimeout: 10,
				},
			},
			want: aws.Int64(10),
		},
		{
			name: "shouldGetNilVisibilityTimeoutWhenNoConfigSpecified",
			fields: fields{
				config: &SQSConf{
					Queue: "queue-test",
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SQS{
				config: tt.fields.config,
				sqs:    tt.fields.sqs,
			}
			got := s.getVisibilityTimeout()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSQS_handleMessagesWhenSqsError(t *testing.T) {
	ctx := context.Background()
	type fields struct {
		config *SQSConf
		sqs    sqsiface.SQSAPI
	}
	type args struct {
		ctx       context.Context
		consumeFn ConsumerFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "should error when receive error",
			fields: fields{
				config: &SQSConf{
					Queue:             "queue",
					VisibilityTimeout: 0,
				},
				sqs: NewSqsMock(fmt.Errorf("error"), nil),
			},
			args: args{
				ctx: ctx,
				consumeFn: func(data []byte) error {
					return nil
				},
			},
		},
		{
			name: "should error when delete error",
			fields: fields{
				config: &SQSConf{
					Queue:             "queue",
					VisibilityTimeout: 0,
				},
				sqs: NewSqsMock(nil, fmt.Errorf("error")),
			},
			args: args{
				ctx: ctx,
				consumeFn: func(data []byte) error {
					return nil
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SQS{
				config: tt.fields.config,
				sqs:    tt.fields.sqs,
			}
			require.Error(t, s.handleMessages(tt.args.ctx, tt.args.consumeFn))
		})
	}
}

func getQueueContent() *sqs.ReceiveMessageOutput {
	return &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				MessageId: aws.String("msg1"),
				Body:      aws.String("msg1"),
			},
			{
				MessageId: aws.String("msg2"),
				Body:      aws.String("msg2"),
			},
			{
				MessageId: aws.String("msg3"),
				Body:      aws.String("msg3"),
			},
		},
	}
}
