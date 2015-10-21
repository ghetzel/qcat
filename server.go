package main

import (
    "fmt"
    "net"
    "net/http"
    "os"
    "strconv"
    "time"

    "github.com/ghetzel/qcat/util"
    log "github.com/Sirupsen/logrus"
    "github.com/julienschmidt/httprouter"
    "gopkg.in/unrolled/render.v1"
)

const (
    DEFAULT_LISTEN_ADDR = `0.0.0.0`
    DEFAULT_LISTEN_PORT = 17684
)

type HttpServer struct {
    Address      string
    Port         int
    BaseHeader   MessageHeader

    amqp         *AmqpClient
    renderer     *render.Render
    router       *httprouter.Router
}

func NewHttpServer(address string, port int, amqpClient *AmqpClient) *HttpServer {
    rv := &HttpServer{
        Address:  address,
        Port:     port,
        amqp:     amqpClient,
        renderer: render.New(),
        router:   httprouter.New(),
    }

    rv.loadRoutes()

    return rv
}



func (self *HttpServer) Run() error {
    listenAddr := fmt.Sprintf("%s:%d", self.Address, self.Port)

    log.Infof("Starting HTTP API server at %s", listenAddr)

    if listener, err := net.Listen("tcp", listenAddr); err == nil {
        if err := http.Serve(listener, self.router); err != nil {
            return fmt.Errorf("Cannot start API server: %s", err)
        }
    }else{
        return err
    }

    return nil
}

func (self *HttpServer) loadRoutes() {
    self.router.GET(`/api/status`, func(w http.ResponseWriter, req *http.Request, params httprouter.Params){
        hostname, _ := os.Hostname()

        self.Respond(w, http.StatusOK, map[string]interface{}{
            `started_at`:  util.StartedAt.Format(time.RFC3339),
            `node`:        hostname,
            `application`: util.ApplicationName,
            `version`:     util.ApplicationVersion,
            `amqp`:        map[string]interface{}{
                `host`:        self.amqp.Host,
                `port`:        self.amqp.Port,
                `vhost`:       self.amqp.Vhost,
                `exchange`:    self.amqp.ExchangeName,
                `routing_key`: self.amqp.RoutingKey,
                `queue`:       self.amqp.QueueName,
                `durable`:     self.amqp.Durable,
                `autodelete`:  self.amqp.Autodelete,
                `exclusive`:   self.amqp.Exclusive,
                `mandatory`:   self.amqp.Mandatory,
                `immediate`:   self.amqp.Immediate,
            },
        }, nil)
    })

    self.router.POST(`/api/publish`, func(w http.ResponseWriter, req *http.Request, params httprouter.Params){
        header := self.BaseHeader

        if v := req.Header.Get(`content-type`); v != `` {
            header.ContentType = v
        }

        if v := req.Header.Get(`content-encoding`); v != `` {
            header.ContentEncoding = v
        }

        if v := req.URL.Query().Get(`ttl`); v != `` {
            header.Expiration = v
        }

        if v := req.URL.Query().Get(`persistent`); v == `true` {
            header.DeliveryMode = uint8(2)
        }

        if v := req.URL.Query().Get(`priority`); v != `` {
            if vi, err := strconv.ParseInt(v, 10, 8); err == nil {
                header.Priority = uint8(vi)
            }
        }

        if err := self.amqp.Publish(req.Body, header); err != nil {
            self.Respond(w, http.StatusServiceUnavailable, nil, fmt.Errorf("Error publishing: %v", err))
            return
        }

        self.Respond(w, http.StatusNoContent, nil, nil)
    })
}


func (self *HttpServer) Respond(w http.ResponseWriter, code int, payload interface{}, err error) {
    response := make(map[string]interface{})
    response["responded_at"] = time.Now().Format(time.RFC3339)
    response["payload"] = payload

    if code >= http.StatusBadRequest {
        response["success"] = false

        if err != nil {
            response["error"] = err.Error()
        }
    }else{
        response["success"] = true
    }

    self.renderer.JSON(w, code, response)
}