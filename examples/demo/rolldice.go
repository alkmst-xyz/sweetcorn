package main

import (
	"io"
	"math/rand"
	"net/http"
	"strconv"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const name = "github.com/alkmst-xyz/sweetcorn/examples/demo"

var (
	tracer = otel.Tracer(name)
	meter  = otel.Meter(name)
	logger = otelslog.NewLogger(name)
	// rollCounter metric.Int64Counter
)

// func init() {
// 	var err error
// 	rollCnt, err = meter.Int64Counter("dice.rolls",
// 		metric.WithDescription("The number of rolls by roll value"),
// 		metric.WithUnit("{roll}"))
// 	if err != nil {
// 		panic(err)
// 	}
// }

type Rolldice struct {
	rollCounter metric.Int64Counter
}

func NewRolldice() Rolldice {
	rollCounter, err := meter.Int64Counter("dice.rolls",
		metric.WithDescription("Rolls by value"),
		metric.WithUnit("{roll}"))
	if err != nil {
		panic(err)
	}

	return Rolldice{
		rollCounter: rollCounter,
	}
}

func (s *Rolldice) rolldice(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "roll")
	defer span.End()

	roll := 1 + rand.Intn(6)

	var msg string
	if player := r.PathValue("player"); player != "" {
		msg = player + " is rolling the dice"
	} else {
		msg = "Anonymous player is rolling the dice"
	}
	logger.InfoContext(ctx, msg, "result", roll)

	rollValueAttr := attribute.Int("roll.value", roll)
	span.SetAttributes(rollValueAttr)
	s.rollCounter.Add(ctx, 1, metric.WithAttributes(rollValueAttr))

	resp := strconv.Itoa(roll) + "\n"
	if _, err := io.WriteString(w, resp); err != nil {
		logger.ErrorContext(ctx, "Write failed", "error", err)
	}
}
