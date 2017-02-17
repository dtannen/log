package firehose

import (
	"encoding/json"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	fh "github.com/dtannen/go-firehose"
	"github.com/rogpeppe/fastuuid"
)

// Handler implementation.
type Handler struct {
	appName  string
	producer *fh.FirehoseProducer
	gen      *fastuuid.Generator
}

// New handler sending logs to Kinesis. To configure producer options or pass your
// own AWS Kinesis client use NewConfig instead.
func New(stream string) *Handler {
	return NewConfig(fh.FirehoseConfig{
		StreamName: stream,
		Client:     firehose.New(session.New(aws.NewConfig())),
	})
}

// NewConfig handler sending logs to Kinesis. The `config` given is passed to the batch
// Kinesis producer, and a random value is used as the partition key for even distribution.
func NewConfig(config fh.FirehoseConfig) *Handler {
	producer := fh.NewFirehose(config)
	producer.Start()
	return &Handler{
		producer: producer,
		gen:      fastuuid.MustNewGenerator(),
	}
}

// HandleLog implements log.Handler.
func (h *Handler) HandleLog(e *log.Entry) error {
	b, err := json.Marshal(e.Fields)
	if err != nil {
		return err
	}

	return h.producer.Put(b)
}
