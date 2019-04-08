package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/iahmedov/eventagg"
	"github.com/iahmedov/eventagg/pkg/aggregator"
	localmq "github.com/iahmedov/eventagg/pkg/mq/local"

	"github.com/go-kit/kit/log"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

type Config struct {
	Port        int
	Queue       *localmq.Queue
	Aggregators map[string]aggregator.View
}

type apiServer struct {
	*http.Server
	conf   Config
	logger log.Logger
}

type aggregateViewRequest struct {
	Aggregator aggregator.View
	Params     []aggregator.Param
}

var emptyData = struct{}{}

func New(cfg Config, logger log.Logger) *apiServer {
	router := httprouter.New()
	srv := &apiServer{
		conf:   cfg,
		logger: logger,
	}

	router.POST("/api/v1/event", srv.InsertEvent)
	router.GET("/api/v1/aggregator/:name", srv.ViewAggregate)

	srv.Server = &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: router,
	}
	return srv
}

func (s *apiServer) decodeInsertEvent(r *http.Request) (*eventagg.Event, error) {
	raw, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, newError("network", errors.Wrap(err, "failed to read content").Error())
	}

	var ev eventagg.Event
	if err = json.Unmarshal(raw, &ev); err != nil {
		return nil, newError("data", errors.Wrap(err, "failed to parse content").Error())
	}
	return &ev, nil
}

func (s *apiServer) InsertEvent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ev, err := s.decodeInsertEvent(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}

	s.logger.Log("event", "incoming event", "data", ev)
	err = s.conf.Queue.Insert(ev)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}
	respondJSON(w, http.StatusAccepted, emptyData)
}

func (s *apiServer) decodeViewAggregate(r *http.Request, params httprouter.Params) (*aggregateViewRequest, error) {
	aggregateName := params.ByName("name")
	if aggregateName == "" {
		return nil, newError("aggregate_name", "aggregate name not given")
	}

	agg, ok := s.conf.Aggregators[aggregateName]
	if !ok {
		return nil, newError("param", fmt.Sprintf("no aggregator with name: %s", aggregateName))
	}

	queryParams := make([]aggregator.Param, 0)
	for k, v := range r.URL.Query() {
		queryParams = append(queryParams, aggregator.Param{
			Key:   k,
			Value: strings.Join(v, ","),
		})
	}

	return &aggregateViewRequest{
		Aggregator: agg,
		Params:     queryParams,
	}, nil
}

func (s *apiServer) ViewAggregate(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	req, err := s.decodeViewAggregate(r, params)
	if err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}

	res, err := req.Aggregator.View(req.Params...)
	if err != nil {
		respondError(w, http.StatusInternalServerError, newError("aggregator", err.Error()))
		return
	}
	respondJSON(w, http.StatusOK, res)
}

func respondError(w http.ResponseWriter, statusCode int, errs ...error) error {
	if len(errs) == 0 {
		return respondJSON(w, statusCode, emptyData)
	}

	return respondJSON(w, statusCode, map[string][]error{
		"errors": errs,
	})
}

func respondJSON(w http.ResponseWriter, statusCode int, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	return json.NewEncoder(w).Encode(data)
}

func (s *apiServer) Run(ctx context.Context) error {
	errChan := make(chan error, 1)
	go func() {
		s.logger.Log("event", "starting server", "port", s.conf.Port)
		if err := s.ListenAndServe(); err != nil {
			s.logger.Log("event", "listen and serve finished", "error", err)
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case err := <-errChan:
		s.logger.Log("event", "api server shutdown", "error", err)
		return err
	case _ = <-ctx.Done():
		s.logger.Log("event", "context done")
		c, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		s.Shutdown(c)
	}
	return nil
}
