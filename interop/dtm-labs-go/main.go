package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/go-resty/resty/v2"
)

type nativeResponse struct {
	Code int               `json:"code"`
	Data nativeTransaction `json:"data"`
}

type nativeTransaction struct {
	GID    string `json:"gid"`
	Status string `json:"status"`
}

func main() {
	baseURL := env("ROZE_DTM_BASE_URL", "http://127.0.0.1:18090")
	token := required("ROZE_DTM_CONTROL_TOKEN")
	dtmServer := strings.TrimRight(baseURL, "/") + "/api/dtmsvr"
	var branchCalls atomic.Uint64
	var tccTryCalls atomic.Uint64
	var tccConfirmCalls atomic.Uint64
	var tccFailedTryCalls atomic.Uint64
	var tccCancelCalls atomic.Uint64
	branchServer := &http.Server{
		Addr:              "127.0.0.1:18091",
		ReadHeaderTimeout: 2 * time.Second,
		Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			branchCalls.Add(1)
			response.Header().Set("content-type", "application/json")
			switch request.URL.Path {
			case "/upstream/tcc/try":
				tccTryCalls.Add(1)
			case "/upstream/tcc/confirm":
				tccConfirmCalls.Add(1)
			case "/upstream/tcc-failure/try":
				tccFailedTryCalls.Add(1)
				response.WriteHeader(http.StatusConflict)
				_, _ = response.Write([]byte(`{"dtm_result":"FAILURE"}`))
				return
			case "/upstream/tcc-failure/cancel":
				tccCancelCalls.Add(1)
			}
			_, _ = response.Write([]byte(`{"dtm_result":"SUCCESS"}`))
		}),
	}
	go func() {
		if err := branchServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	defer branchServer.Shutdown(context.Background())
	waitBranchServer()

	dtmcli.GetRestyClient().SetHeader("Authorization", "Bearer "+token)
	tccGID := dtmcli.MustGenGid(dtmServer)
	must(dtmcli.TccGlobalTransaction(dtmServer, tccGID, func(tcc *dtmcli.Tcc) (*resty.Response, error) {
		return tcc.CallBranch(
			map[string]string{"source": "dtm-labs-go"},
			"http://127.0.0.1:18091/upstream/tcc/try",
			"http://127.0.0.1:18091/upstream/tcc/confirm",
			"http://127.0.0.1:18091/upstream/tcc/cancel",
		)
	}))
	waitStatus(baseURL, token, tccGID, "succeeded")

	tccFailureGID := dtmcli.MustGenGid(dtmServer)
	err := dtmcli.TccGlobalTransaction(dtmServer, tccFailureGID, func(tcc *dtmcli.Tcc) (*resty.Response, error) {
		return tcc.CallBranch(
			map[string]string{"source": "dtm-labs-go", "outcome": "failure"},
			"http://127.0.0.1:18091/upstream/tcc-failure/try",
			"http://127.0.0.1:18091/upstream/tcc-failure/confirm",
			"http://127.0.0.1:18091/upstream/tcc-failure/cancel",
		)
	})
	if !errors.Is(err, dtmcli.ErrFailure) {
		panic(fmt.Errorf("official TCC failure returned %v instead of ErrFailure", err))
	}
	waitStatus(baseURL, token, tccFailureGID, "aborted")

	sagaGID := dtmcli.MustGenGid(dtmServer)
	saga := dtmcli.NewSaga(dtmServer, sagaGID)
	saga.WaitResult = true
	saga.Add("http://127.0.0.1:18091/upstream/saga/action", "http://127.0.0.1:18091/upstream/saga/compensate", map[string]string{"source": "dtm-labs-go"})
	must(saga.Submit())
	waitStatus(baseURL, token, sagaGID, "succeeded")

	messageGID := dtmcli.MustGenGid(dtmServer)
	message := dtmcli.NewMsg(dtmServer, messageGID)
	message.WaitResult = true
	message.Add("http://127.0.0.1:18091/upstream/message/action", map[string]string{"source": "dtm-labs-go"})
	must(message.Prepare(""))
	must(message.Submit())
	waitStatus(baseURL, token, messageGID, "succeeded")

	if branchCalls.Load() < 6 {
		panic(fmt.Errorf("official dtm-labs client produced only %d branch calls", branchCalls.Load()))
	}
	for name, calls := range map[string]uint64{
		"tcc_try":        tccTryCalls.Load(),
		"tcc_confirm":    tccConfirmCalls.Load(),
		"tcc_failed_try": tccFailedTryCalls.Load(),
		"tcc_cancel":     tccCancelCalls.Load(),
	} {
		if calls == 0 {
			panic(fmt.Errorf("official dtm-labs TCC did not call %s", name))
		}
	}
	fmt.Printf("{\"verdict\":\"pass\",\"client\":\"dtm-labs-go\",\"revision\":\"18146ee53bafbf094b1a5f12ca7e8a29bdb57edd\",\"transactions\":4,\"modes\":[\"tcc\",\"saga\",\"message\"],\"branch_calls\":%d}\n", branchCalls.Load())
}

func waitStatus(baseURL, token, gid, expected string) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+"/v1/transactions/"+gid, nil)
		must(err)
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		must(err)
		var body nativeResponse
		err = json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		must(err)
		if response.StatusCode != http.StatusOK || body.Code != 0 {
			panic(fmt.Errorf("native transaction query failed: HTTP %d code %d", response.StatusCode, body.Code))
		}
		if strings.EqualFold(body.Data.Status, expected) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic(fmt.Errorf("transaction %s did not reach %s", gid, expected))
}

func waitBranchServer() {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get("http://127.0.0.1:18091/ready")
		if err == nil {
			response.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("dtm-labs Go branch server did not start")
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
