package httpresponse_test

import (
	"net/http"
	"testing"

	"github.com/kumparan/go-lib/http/httpresponse"
)

func TestStatusOK(t *testing.T) {
	w := &http.ResponseWriter{}
	httpresponse.StatusOK()
}
