package consumer

import "context"

type ConsumerFn func(data []byte) error

type ConsumerBatchFn func(data [][]byte) error

type DataSource interface {
	Start(ctx context.Context, consumeFn ConsumerFn) error
}
