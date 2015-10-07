package main

import (
    "fmt"
    "os"
    log "github.com/Sirupsen/logrus"
    "github.com/codegangsta/cli"
    "github.com/ghetzel/amqpcat/util"
)

const (
    DEFAULT_LOGLEVEL  = `info`
)

func parseLogLevel(logLevel string) {
    log.SetOutput(os.Stderr)
    log.SetFormatter(&log.TextFormatter{
        ForceColors: true,
    })

    switch logLevel {
    case `info`:
        log.SetLevel(log.InfoLevel)
    case `warn`:
        log.SetLevel(log.WarnLevel)
    case `error`:
        log.SetLevel(log.ErrorLevel)
    case `fatal`:
        log.SetLevel(log.FatalLevel)
    case `quiet`:
        log.SetLevel(log.PanicLevel)
    default:
        log.SetLevel(log.DebugLevel)
    }
}

func CreateClient(c *cli.Context) (*Client, error) {
    if len(c.Args()) > 0 {
        if client, err := NewClient(c.Args()[0]); err == nil {
            client.Autodelete = c.Bool(`autodelete`)
            client.Durable    = c.Bool(`durable`)
            client.Exclusive  = c.Bool(`exclusive`)
            client.Immediate  = c.Bool(`immediate`)
            client.Mandatory  = c.Bool(`mandatory`)
            client.ID         = c.String(`consumer`)
            client.QueueName  = c.String(`queue`)
            client.RoutingKey = c.String(`routing-key`)

            log.Debugf("Connecting to %s:%d vhost=%s queue=%s", client.Host, client.Port, client.Vhost, client.QueueName)

            if err := client.Connect(); err == nil {
                return client, nil
            }else{
                return nil, fmt.Errorf("Error connecting to consumer: %v", err)
            }
        }else{
            return nil, fmt.Errorf("Error initializing consumer: %v", err)
        }
    }else{
        return nil, fmt.Errorf("Must provide an AMQP connection URI as an argument")
    }
}

func main(){
    app                      := cli.NewApp()
    app.Name                  = util.ApplicationName
    app.Usage                 = util.ApplicationSummary
    app.Version               = util.ApplicationVersion
    app.EnableBashCompletion  = false
    app.Before                = func(c *cli.Context) error {
        parseLogLevel(c.String(`log-level`))
        return nil
    }

    app.Flags = []cli.Flag{
        cli.StringFlag{
            Name:   `log-level, L`,
            Usage:  `Level of log output verbosity`,
            Value:  DEFAULT_LOGLEVEL,
            EnvVar: `LOGLEVEL`,
        },
    }

    app.Commands = []cli.Command{
        {
            Name:   `publish`,
            Usage:  `Connect to an AMQP message broker and submit messages read from standard input`,
            Flags:  []cli.Flag{
                cli.StringFlag{
                    Name:   `exchange, e`,
                    Usage:  `The name of the exchange to bind to`,
                },
                cli.StringFlag{
                    Name:   `routing-key, r`,
                    Usage:  `The routing key to use when publishing messages`,
                    Value:  DEFAULT_QUEUE_NAME,
                },
                cli.BoolFlag{
                    Name:   `durable, D`,
                    Usage:  `Durable queues will survive server restarts and remain when there are no remaining consumers or bindings`,
                },
                cli.BoolFlag{
                    Name:   `autodelete, A`,
                    Usage:  `Auto-deleted queues will be automatically removed when all clients disconnect`,
                },
                cli.BoolFlag{
                    Name:   `exclusive, E`,
                    Usage:  `Exclusive queues are only accessible by the connection that declares them and will be deleted when the connection closes`,
                },
                cli.BoolFlag{
                    Name:   `mandatory, M`,
                    Usage:  `Messages are undeliverable when the mandatory flag is true and no queue is bound that matches the routing key`,
                },
                cli.BoolFlag{
                    Name:   `immediate, I`,
                    Usage:  `Messages are undeliverable when the immediate flag is true and no consumer on the matched queue is ready to accept the delivery`,
                },
            },
            Action: func(c *cli.Context) {
                if client, err := CreateClient(c); err == nil {
                    if err := client.Publish(os.Stdin); err != nil {
                        log.Fatalf("Error publishing: %v", err)
                    }
                }else{
                    log.Fatalf("%v", err)
                }
            },
        },{
            Name:   `consume`,
            Usage:  `Connect to an AMQP message broker and print messages to standard output`,
            Flags:  []cli.Flag{
                cli.StringFlag{
                    Name:   `consumer, C`,
                    Usage:  `The consumer name to report to the broker`,
                },
                cli.StringFlag{
                    Name:   `queue, Q`,
                    Usage:  `The name of the queue to bind to`,
                    Value:  DEFAULT_QUEUE_NAME,
                },
                cli.BoolFlag{
                    Name:   `durable, D`,
                    Usage:  `Durable queues will survive server restarts and remain when there are no remaining consumers or bindings`,
                },
                cli.BoolFlag{
                    Name:   `autodelete, A`,
                    Usage:  `Auto-deleted queues will be automatically removed when all clients disconnect`,
                },
                cli.BoolFlag{
                    Name:   `exclusive, E`,
                    Usage:  `Exclusive queues are only accessible by the connection that declares them and will be deleted when the connection closes`,
                },
            },
            Action: func(c *cli.Context) {
                if client, err := CreateClient(c); err == nil {
                    if msgs, err := client.Subscribe(); err == nil {
                        for msg := range msgs {
                            fmt.Println(msg)
                        }
                    }else{
                        log.Fatalf("Error subscribing: %v", err)
                    }
                }else{
                    log.Fatalf("%v", err)
                }
            },
        },{
            Name:   `version`,
            Usage:  `Output the current version and exit`,
            Action: func (c *cli.Context) {
                fmt.Println(util.ApplicationVersion)
            },
        },
    }

//  load plugin subcommands
    // app.Commands = append(app.Commands, api.Register()...)

    app.Run(os.Args)
}