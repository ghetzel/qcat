package qcat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/streadway/amqp"
)

var DefaultQueueName = `qcat`

type AmqpClient struct {
	ID                string
	Host              string
	Port              int
	Username          string
	Password          string
	Vhost             string
	ExchangeName      string
	RoutingKey        string
	QueueName         string
	Durable           bool
	Autodelete        bool
	Exclusive         bool
	Mandatory         bool
	Immediate         bool
	conn              *amqp.Connection
	channel           *amqp.Channel
	queue             amqp.Queue
	uri               amqp.URI
	outchan           chan *Message
	downstreamErrchan chan *amqp.Error
	errchan           chan error
}

type DeliveryMode int

const (
	Transient DeliveryMode = iota
	Persistent
)

type MessageHeader struct {
	ContentType     string
	ContentEncoding string
	DeliveryMode    DeliveryMode
	Priority        int
	Expiration      time.Duration
}

type Message struct {
	Timestamp time.Time
	Header    MessageHeader
	Body      []byte
}

func (self *Message) Decode(into interface{}) error {
	switch self.Header.ContentType {
	case `application/json`:
		return json.Unmarshal(self.Body, into)
	default:
		if b, ok := into.([]byte); ok {
			if n := copy(b, self.Body); n == 0 && len(self.Body) > 0 {
				return fmt.Errorf("target must be able to hold at least %d bytes", len(self.Body))
			} else {
				return nil
			}
		} else {
			return typeutil.SetValue(into, string(self.Body))
		}
	}
}

func NewAmqpClient(uri string) (*AmqpClient, error) {
	c := &AmqpClient{
		QueueName:         DefaultQueueName,
		outchan:           make(chan *Message),
		downstreamErrchan: make(chan *amqp.Error),
		errchan:           make(chan error),
	}

	if u, err := amqp.ParseURI(uri); err == nil {
		c.uri = u
		c.Host = u.Host
		c.Port = u.Port
		c.Username = u.Username
		c.Password = u.Password
		c.Vhost = u.Vhost

		return c, nil
	} else {
		return nil, err
	}
}

func (self *AmqpClient) Close() error {
	if self.conn == nil {
		return fmt.Errorf("Cannot close, connection does not exist")
	}

	return self.conn.Close()
}

func (self *AmqpClient) Connect() error {
	if conn, err := amqp.Dial(self.uri.String()); err == nil {
		self.conn = conn

		if channel, err := self.conn.Channel(); err == nil {
			self.channel = channel

			// setup error notifications
			go func() {
				for qerr := range self.channel.NotifyClose(self.downstreamErrchan) {
					if qerr.Server {
						self.errchan <- fmt.Errorf("server error %d: %v", qerr.Code, qerr.Reason)
					} else {
						self.errchan <- fmt.Errorf("client error %d: %v", qerr.Code, qerr.Reason)
					}
				}
			}()

			//  consumers will declare a queue
			if self.QueueName != `` {
				if queue, err := self.channel.QueueDeclare(self.QueueName, self.Durable, self.Autodelete, self.Exclusive, false, nil); err == nil {
					self.queue = queue
					return nil
				} else {
					defer self.channel.Close()
					return err
				}
			}
		} else {
			defer self.conn.Close()
			return err
		}
	} else {
		return err
	}

	return nil
}

func (self *AmqpClient) SubscribeRaw() (<-chan amqp.Delivery, error) {
	return self.channel.Consume(self.queue.Name, self.ID, true, self.Exclusive, false, false, nil)
}

// Publish messages read from the given reader, separated by newlines ("\n").
func (self *AmqpClient) PublishLines(reader io.Reader, header MessageHeader) error {
	inScanner := bufio.NewScanner(reader)

	for inScanner.Scan() {
		if err := self.Publish([]byte(inScanner.Bytes()), header); err != nil {
			return err
		}
	}

	return inScanner.Err()
}

// Publish a single message.
func (self *AmqpClient) Publish(data []byte, header MessageHeader) error {
	var deliveryMode int

	switch header.DeliveryMode {
	case Transient:
		deliveryMode = 1
	case Persistent:
		deliveryMode = 2
	}

	return self.channel.Publish(self.ExchangeName, self.RoutingKey, self.Mandatory, self.Immediate, amqp.Publishing{
		Body:            data,
		ContentType:     header.ContentType,
		ContentEncoding: header.ContentEncoding,
		DeliveryMode:    uint8(deliveryMode),
		Priority:        uint8(header.Priority),
		Expiration: fmt.Sprintf("%d", int(
			header.Expiration.Round(time.Millisecond)/time.Millisecond,
		)),
	})
}

// Receive a message from the channel.
func (self *AmqpClient) Subscribe() error {
	if msgs, err := self.SubscribeRaw(); err == nil {
		go func() {
			for delivery := range msgs {
				var deliveryMode DeliveryMode

				switch delivery.DeliveryMode {
				case 2:
					deliveryMode = Persistent
				default:
					deliveryMode = Transient
				}

				self.outchan <- &Message{
					Timestamp: delivery.Timestamp,
					Body:      delivery.Body,
					Header: MessageHeader{
						ContentType:     delivery.ContentType,
						ContentEncoding: delivery.ContentEncoding,
						DeliveryMode:    deliveryMode,
						Priority:        int(delivery.Priority),
					},
				}
			}

			close(self.outchan)
		}()

		return nil
	} else {
		return err
	}
}

// Receive a single message.
func (self *AmqpClient) Receive() <-chan *Message {
	return self.outchan
}

// Receive a single error.
func (self *AmqpClient) Err() <-chan error {
	return self.errchan
}
