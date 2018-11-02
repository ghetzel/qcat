package main

import (
	"fmt"
	"os"

	"github.com/ghetzel/cli"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/qcat"
)

func createAmqpClient(c *cli.Context) (*qcat.AMQP, error) {
	if len(c.Args()) > 0 {
		if client, err := qcat.NewAMQP(c.Args()[0]); err == nil {
			client.ConnectTimeout = c.Duration(`connect-timeout`)
			client.Autodelete = c.Bool(`autodelete`)
			client.Durable = c.Bool(`durable`)
			client.Exclusive = c.Bool(`exclusive`)
			client.Immediate = c.Bool(`immediate`)
			client.Mandatory = c.Bool(`mandatory`)
			client.ID = c.String(`consumer`)
			client.QueueName = c.String(`queue`)
			client.ExchangeName = c.String(`exchange`)
			client.RoutingKey = c.String(`routing-key`)
			client.Prefetch = c.Int(`prefetch`)
			client.HeartbeatInterval = c.Duration(`heartbeat`)

			for _, property := range c.StringSlice(`property`) {
				key, value := stringutil.SplitPair(property, `=`)
				client.ClientProperties[key] = stringutil.Autotype(value)
			}

			log.Debugf("Connecting to %s:%d vhost=%s queue=%s", client.Host, client.Port, client.Vhost, client.QueueName)

			if err := client.Connect(); err == nil {
				return client, nil
			} else {
				return nil, fmt.Errorf("Error connecting to consumer: %v", err)
			}
		} else {
			return nil, fmt.Errorf("Error initializing consumer: %v", err)
		}
	} else {
		return nil, fmt.Errorf("Must provide an AMQP connection URI as an argument")
	}
}

func FlagsForConsumers() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  `consumer, C`,
			Usage: `The consumer name to report to the broker`,
		},
		cli.StringFlag{
			Name:  `queue, Q`,
			Usage: `The name of the queue to bind to`,
			Value: qcat.DefaultQueueName,
		},
		cli.IntFlag{
			Name:  `prefetch, p`,
			Usage: `The number of items to prefetch from the queue`,
			Value: 1,
		},
	}
}

func FlagsForPublishers() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  `exchange, e`,
			Usage: `The name of the exchange to bind to`,
		},
		cli.StringFlag{
			Name:  `routing-key, r`,
			Usage: `The routing key to use when publishing messages`,
			Value: qcat.DefaultQueueName,
		},
		cli.StringFlag{
			Name:  `content-type`,
			Usage: `The Content-Type header to include with published messages`,
		},
		cli.StringFlag{
			Name:  `content-encoding`,
			Usage: `The Content-Encoding header to include with published messages`,
		},
		cli.DurationFlag{
			Name:  `ttl, t`,
			Usage: `The maximum amount of time the message will live in a queue before being automatically deleted`,
		},
		cli.IntFlag{
			Name:  `priority`,
			Usage: `The priority level (0-9) that will be supplied with the published message`,
		},
		cli.BoolFlag{
			Name:  `mandatory, M`,
			Usage: `Messages are undeliverable when the mandatory flag is true and no queue is bound that matches the routing key`,
		},
		cli.BoolFlag{
			Name:  `immediate, I`,
			Usage: `Messages are undeliverable when the immediate flag is true and no consumer on the matched queue is ready to accept the delivery`,
		},
		cli.BoolFlag{
			Name:  `persistent, P`,
			Usage: `Persistent messages are written to disk such that in the event of a broker crash the message is not lost`,
		},
	}
}

func FlagsCommon() []cli.Flag {
	return []cli.Flag{
		cli.BoolFlag{
			Name:  `durable, D`,
			Usage: `Durable queues will survive server restarts and remain when there are no remaining consumers or bindings`,
		},
		cli.BoolFlag{
			Name:  `autodelete, A`,
			Usage: `Auto-deleted queues will be automatically removed when all clients disconnect`,
		},
		cli.BoolFlag{
			Name:  `exclusive, E`,
			Usage: `Exclusive queues are only accessible by the connection that declares them and will be deleted when the connection closes`,
		},
		cli.DurationFlag{
			Name:  `heartbeat`,
			Usage: `Specify on what interval to send heartbeat pings.`,
		},
		cli.StringSliceFlag{
			Name:  `property, c`,
			Usage: `Specify a client property to send to the server as a key=value pair.`,
		},
		cli.DurationFlag{
			Name:  `connect-timeout, T`,
			Usage: `How long to wait before timing out a connection attempt.`,
			Value: qcat.DefaultConnectTimeout,
		},
	}
}

func headerFromContext(c *cli.Context) qcat.MessageHeader {
	header := qcat.MessageHeader{}

	if c.IsSet(`persistent`) {
		if c.Bool(`persistent`) {
			header.DeliveryMode = qcat.Persistent
		} else {
			header.DeliveryMode = qcat.Transient
		}
	}

	if c.Duration(`ttl`) > 0 {
		header.Expiration = c.Duration(`ttl`)
	}

	if c.IsSet(`priority`) {
		header.Priority = c.Int(`priority`)
	}

	if c.IsSet(`content-type`) {
		header.ContentType = c.String(`content-type`)
	}

	if c.IsSet(`content-encoding`) {
		header.ContentEncoding = c.String(`content-encoding`)
	}

	return header
}

func main() {
	app := cli.NewApp()
	app.Name = `qcat`
	app.Usage = `utility for publishing and consuming data from an AMQP message broker`
	app.Version = qcat.Version
	app.EnableBashCompletion = true

	app.Before = func(c *cli.Context) error {
		log.SetLevelString(c.String(`log-level`))
		return nil
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   `log-level, L`,
			Usage:  `Level of log output verbosity`,
			Value:  `info`,
			EnvVar: `LOGLEVEL`,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:      `publish`,
			Usage:     `Connect to an AMQP message broker and submit messages read from standard input`,
			Flags:     append(FlagsCommon(), FlagsForPublishers()...),
			ArgsUsage: `AMQP_URI`,
			Action: func(c *cli.Context) {
				if client, err := createAmqpClient(c); err == nil {
					header := headerFromContext(c)

					if err := client.PublishLines(os.Stdin, header); err != nil {
						log.Fatalf("Error publishing: %v", err)
					}
				} else {
					log.Fatalf("%v", err)
				}
			},
		}, {
			Name:      `consume`,
			Usage:     `Connect to an AMQP message broker and print messages to standard output`,
			Flags:     append(FlagsCommon(), FlagsForConsumers()...),
			ArgsUsage: `AMQP_URI`,
			Action: func(c *cli.Context) {
				if client, err := createAmqpClient(c); err == nil {
					if err := client.Subscribe(); err == nil {
						for {
							select {
							case message := <-client.Receive():
								var line string

								if err := message.Decode(&line); err == nil {
									fmt.Println(line)
								} else {
									log.Fatalf("failed to decode message: %v", err)
								}

							case err := <-client.Err():
								log.Fatal(err)
							}
						}
					} else {
						log.Fatalf("Error subscribing: %v", err)
					}
				} else {
					log.Fatalf("%v", err)
				}
			},
		}, {
			Name:      `serve`,
			Usage:     `Start an HTTP server for receiving and consuming messages from an AMQP message broker`,
			ArgsUsage: `AMQP_URI`,
			Flags: append([]cli.Flag{
				cli.StringFlag{
					Name:  `address, a`,
					Usage: `The address to listen on`,
					Value: qcat.DefaultServerAddress,
				},
			}, append(FlagsCommon(), append(FlagsForPublishers(), FlagsForConsumers()...)...)...),
			Action: func(c *cli.Context) {
				if client, err := createAmqpClient(c); err == nil {
					server := qcat.NewHttpServer(client)
					server.BaseHeader = headerFromContext(c)

					if err := server.ListenAndServe(c.String(`address`)); err != nil {
						log.Fatalf("%v", err)
					}
				} else {
					log.Fatalf("%v", err)
				}
			},
		}, {
			Name:  `version`,
			Usage: `Output the current version and exit`,
			Action: func(c *cli.Context) {
				fmt.Println(app.Version)
			},
		},
	}

	//  load plugin subcommands
	// app.Commands = append(app.Commands, api.Register()...)

	app.Run(os.Args)
}
