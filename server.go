package qcat

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/unrolled/render.v1"
)

var DefaultServerAddress = `:17684`

type HttpServer struct {
	BaseHeader MessageHeader
	amqp       *AMQP
	renderer   *render.Render
	router     *httprouter.Router
}

func NewHttpServer(amqpClient *AMQP) *HttpServer {
	rv := &HttpServer{
		amqp:     amqpClient,
		renderer: render.New(),
		router:   httprouter.New(),
	}

	return rv
}

func (self *HttpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	self.router.ServeHTTP(w, req)
}

func (self *HttpServer) ListenAndServe(address string) error {
	self.loadRoutes()
	log.Infof("Starting HTTP API server at %s", address)

	if listener, err := net.Listen("tcp", address); err == nil {
		if err := http.Serve(listener, self.router); err != nil {
			return fmt.Errorf("Cannot start API server: %s", err)
		}
	} else {
		return err
	}

	return nil
}

func (self *HttpServer) loadRoutes() {
	self.router.GET(`/api/status`, func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		hostname, _ := os.Hostname()

		self.Respond(w, http.StatusOK, map[string]interface{}{
			`node`: hostname,
			`amqp`: map[string]interface{}{
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

	self.router.POST(`/api/publish`, func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		header := self.BaseHeader

		header.ContentType = httputil.Q(req, `content-type`)
		header.ContentEncoding = httputil.Q(req, `content-encoding`)
		header.Priority = int(httputil.QInt(req, `priority`))

		if expr := httputil.QInt(req, `ttl`); expr > 0 {
			header.Expiration = time.Duration(expr) * time.Millisecond
		}

		if v := req.URL.Query().Get(`persistent`); v == `true` {
			header.DeliveryMode = Transient
		}

		if httputil.QBool(req, `lines`) {
			if err := self.amqp.PublishLines(req.Body, header); err != nil {
				self.Respond(w, http.StatusServiceUnavailable, nil, fmt.Errorf("Error publishing: %v", err))
				return
			}
		} else if data, err := ioutil.ReadAll(req.Body); err == nil {
			if err := self.amqp.Publish(data, header); err != nil {
				self.Respond(w, http.StatusServiceUnavailable, nil, fmt.Errorf("Error publishing: %v", err))
				return
			}
		} else {
			self.Respond(w, http.StatusBadRequest, nil, err)
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
	} else {
		response["success"] = true
	}

	self.renderer.JSON(w, code, response)
}
