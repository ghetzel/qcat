package main

import (
    "bufio"
    "fmt"
    "io"
    "github.com/streadway/amqp"
)

const (
    DEFAULT_AMQP_PORT  = 5672
    DEFAULT_QUEUE_NAME = `amqpcat`
)

type Client struct {
    ID           string
    Host         string
    Port         int
    Username     string
    Password     string
    Vhost        string
    ExchangeName string
    RoutingKey   string
    QueueName    string

    Durable      bool
    Autodelete   bool
    Exclusive    bool
    Mandatory    bool
    Immediate    bool

    conn         *amqp.Connection
    channel      *amqp.Channel
    queue        amqp.Queue
    uri          amqp.URI
}

type MessageHeader struct {
    ContentType     string
    ContentEncoding string
}

func NewClient(uri string) (*Client, error) {
    c := new(Client)

    if u, err := amqp.ParseURI(uri); err == nil {
        c.uri       = u
        c.Host      = u.Host
        c.Port      = u.Port
        c.Username  = u.Username
        c.Password  = u.Password
        c.Vhost     = u.Vhost
        c.QueueName = DEFAULT_QUEUE_NAME

        return c, nil
    }else{
        return nil, err
    }
}

func (self *Client) Close() error {
    if self.conn == nil {
        return fmt.Errorf("Cannot close, connection does not exist")
    }

    return self.conn.Close()
}

func (self *Client) Connect() error {
    if conn, err := amqp.Dial(self.uri.String()); err == nil {
        self.conn = conn

        if channel, err := self.conn.Channel(); err == nil {
            self.channel = channel

        //  consumers will declare a queue
            if self.QueueName != `` {
                if queue, err := self.channel.QueueDeclare(self.QueueName, self.Durable, self.Autodelete, self.Exclusive, false, nil); err == nil {
                    self.queue = queue
                    return nil
                }else{
                    defer self.channel.Close()
                    return err
                }
            }
        }else{
            defer self.conn.Close()
            return err
        }
    }else{
        return err
    }

    return nil
}

func (self *Client) SubscribeRaw() (<-chan amqp.Delivery, error) {
    return self.channel.Consume(self.queue.Name, self.ID, true, self.Exclusive, false, false, nil)
}

func (self *Client) Publish(reader io.Reader, header MessageHeader) error {
    inScanner := bufio.NewScanner(reader)

    for inScanner.Scan() {
        body := inScanner.Text()
        self.channel.Publish(self.ExchangeName, self.RoutingKey, self.Mandatory, self.Immediate, amqp.Publishing{
            ContentType:     header.ContentType,
            ContentEncoding: header.ContentEncoding,
            Body:            []byte(body[:]),
        })
    }

    return nil
}


func (self *Client) Subscribe() (<-chan string, error) {
    output := make(chan string)

    if msgs, err := self.SubscribeRaw(); err == nil {
        go func(){
            for delivery := range msgs {
                output <- string(delivery.Body[:])
            }
        }()
    }else{
        return nil, err
    }

    return output, nil
}
