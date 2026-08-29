package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
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
	branchServer := &http.Server{
		Addr:              "127.0.0.1:18091",
		ReadHeaderTimeout: 2 * time.Second,
		Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			branchCalls.Add(1)
			response.Header().Set("content-type", "application/json")
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
	sagaGID := dtmcli.MustGenGid(dtmServer)
	saga := dtmcli.NewSaga(dtmServer, sagaGID)
	saga.WaitResult = true
	saga.Add("http://127.0.0.1:18091/upstream/saga/action", "http://127.0.0.1:18091/upstream/saga/compensate", map[string]string{"source": "dtm-labs-go"})
	must(saga.Submit())
	waitSucceeded(baseURL, token, sagaGID)

	messageGID := dtmcli.MustGenGid(dtmServer)
	message := dtmcli.NewMsg(dtmServer, messageGID)
	message.WaitResult = true
	message.Add("http://127.0.0.1:18091/upstream/message/action", map[string]string{"source": "dtm-labs-go"})
	must(message.Prepare(""))
	must(message.Submit())
	waitSucceeded(baseURL, token, messageGID)

	if branchCalls.Load() < 2 {
		panic(fmt.Errorf("official dtm-labs client produced only %d branch calls", branchCalls.Load()))
	}
	fmt.Printf("{\"verdict\":\"pass\",\"client\":\"dtm-labs-go\",\"revision\":\"18146ee53bafbf094b1a5f12ca7e8a29bdb57edd\",\"transactions\":2,\"branch_calls\":%d}\n", branchCalls.Load())
}

func waitSucceeded(baseURL, token, gid string) {
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
		if strings.EqualFold(body.Data.Status, "succeeded") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic(fmt.Errorf("transaction %s did not reach succeeded", gid))
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
