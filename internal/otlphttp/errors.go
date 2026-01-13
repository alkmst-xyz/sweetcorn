// https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/otlpreceiver/internal/errors/errors.go
package otlphttp

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func GetHTTPStatusCodeFromStatus(s *status.Status) int {
	// See https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures
	// to see if a code is retryable.
	// See https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures-1
	// to see a list of retryable http status codes.
	switch s.Code() {
	// Retryable
	case codes.Canceled, codes.DeadlineExceeded, codes.Aborted, codes.OutOfRange, codes.Unavailable, codes.DataLoss:
		return http.StatusServiceUnavailable
	// Retryable
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	// Not Retryable
	case codes.InvalidArgument:
		return http.StatusBadRequest
	// Not Retryable
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	// Not Retryable
	case codes.PermissionDenied:
		return http.StatusForbidden
	// Not Retryable
	case codes.Unimplemented:
		return http.StatusNotFound
	// Not Retryable
	default:
		return http.StatusInternalServerError
	}
}
