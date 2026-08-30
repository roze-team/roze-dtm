package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/go-resty/resty/v2"
	_ "github.com/lib/pq"
)

const xaBarrierTable = "roze_dtm_go_barrier"

type xaHarness struct {
	conf          dtmcli.DBConf
	db            *sql.DB
	actionCalls   atomic.Uint64
	commitCalls   atomic.Uint64
	rollbackCalls atomic.Uint64
}

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
	xa, err := newXAHarness(os.Getenv("ROZE_DTM_TEST_POSTGRES_URL"))
	must(err)
	if strings.EqualFold(os.Getenv("ROZE_DTM_REQUIRE_XA"), "true") && xa == nil {
		panic("ROZE_DTM_REQUIRE_XA requires ROZE_DTM_TEST_POSTGRES_URL")
	}
	if xa != nil {
		defer xa.db.Close()
	}
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
			if request.URL.Path == "/upstream/xa" {
				handleXABranch(response, request, xa)
				return
			}
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
	err = dtmcli.TccGlobalTransaction(dtmServer, tccFailureGID, func(tcc *dtmcli.Tcc) (*resty.Response, error) {
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

	transactions := 4
	modes := []string{"tcc", "saga", "message"}
	if xa != nil {
		runXAInterop(baseURL, token, dtmServer, xa)
		transactions += 2
		modes = append(modes, "xa")
	}

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
	result := map[string]any{
		"verdict":      "pass",
		"client":       "dtm-labs-go",
		"revision":     "18146ee53bafbf094b1a5f12ca7e8a29bdb57edd",
		"transactions": transactions,
		"modes":        modes,
		"branch_calls": branchCalls.Load(),
		"xa_executed":  xa != nil,
	}
	encoded, err := json.Marshal(result)
	must(err)
	fmt.Println(string(encoded))
}

func newXAHarness(rawURL string) (*xaHarness, error) {
	if rawURL == "" {
		return nil, nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse XA PostgreSQL URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return nil, fmt.Errorf("XA database must use postgres or postgresql scheme")
	}
	if parsed.User == nil {
		return nil, fmt.Errorf("XA PostgreSQL URL must include user credentials")
	}
	port := int64(5432)
	if parsed.Port() != "" {
		port, err = strconv.ParseInt(parsed.Port(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse XA PostgreSQL port: %w", err)
		}
	}
	password, _ := parsed.User.Password()
	conf := dtmcli.DBConf{
		Driver:   "postgres",
		Host:     parsed.Hostname(),
		Port:     port,
		User:     parsed.User.Username(),
		Password: password,
		Db:       strings.TrimPrefix(parsed.Path, "/"),
		Schema:   "public",
	}
	if conf.Host == "" || conf.User == "" || conf.Db == "" {
		return nil, fmt.Errorf("XA PostgreSQL URL must include host, user, and database")
	}
	query := parsed.Query()
	if query.Get("sslmode") == "" {
		query.Set("sslmode", "disable")
		parsed.RawQuery = query.Encode()
	}
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		return nil, fmt.Errorf("open XA PostgreSQL database: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping XA PostgreSQL database: %w", err)
	}
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS roze_dtm_go_barrier (
			id BIGSERIAL PRIMARY KEY,
			trans_type VARCHAR(45) DEFAULT '',
			gid VARCHAR(128) DEFAULT '',
			branch_id VARCHAR(128) DEFAULT '',
			op VARCHAR(45) DEFAULT '',
			barrier_id VARCHAR(45) DEFAULT '',
			reason VARCHAR(45) DEFAULT '',
			create_time TIMESTAMPTZ DEFAULT NULL,
			update_time TIMESTAMPTZ DEFAULT NULL,
			CONSTRAINT uniq_barrier UNIQUE (gid, branch_id, op, barrier_id)
		)`,
		`CREATE TABLE IF NOT EXISTS roze_dtm_go_xa_values (
			gid VARCHAR(128) PRIMARY KEY,
			value VARCHAR(64) NOT NULL
		)`,
	} {
		if _, err = db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize XA PostgreSQL fixture: %w", err)
		}
	}
	dtmcli.SetBarrierTableName(xaBarrierTable)
	return &xaHarness{conf: conf, db: db}, nil
}

func handleXABranch(response http.ResponseWriter, request *http.Request, harness *xaHarness) {
	if harness == nil {
		http.Error(response, `{"dtm_result":"FAILURE"}`, http.StatusServiceUnavailable)
		return
	}
	op := request.URL.Query().Get("op")
	err := dtmcli.XaLocalTransaction(request.URL.Query(), harness.conf, func(db *sql.DB, xa *dtmcli.Xa) error {
		harness.actionCalls.Add(1)
		_, err := db.Exec(
			"INSERT INTO roze_dtm_go_xa_values (gid, value) VALUES ($1, $2)",
			xa.Gid,
			"prepared-by-official-go-client",
		)
		return err
	})
	if err != nil {
		response.WriteHeader(http.StatusInternalServerError)
		_, _ = response.Write([]byte(`{"dtm_result":"FAILURE"}`))
		return
	}
	switch op {
	case "commit":
		harness.commitCalls.Add(1)
	case "rollback":
		harness.rollbackCalls.Add(1)
	}
	_, _ = response.Write([]byte(`{"dtm_result":"SUCCESS"}`))
}

func runXAInterop(baseURL, token, dtmServer string, harness *xaHarness) {
	committedGID := dtmcli.MustGenGid(dtmServer)
	must(dtmcli.XaGlobalTransaction(dtmServer, committedGID, func(xa *dtmcli.Xa) (*resty.Response, error) {
		return xa.CallBranch(
			map[string]string{"source": "dtm-labs-go", "outcome": "commit"},
			"http://127.0.0.1:18091/upstream/xa",
		)
	}))
	waitStatus(baseURL, token, committedGID, "succeeded")
	if rows := xaValueCount(harness.db, committedGID); rows != 1 {
		panic(fmt.Errorf("official XA commit persisted %d rows instead of 1", rows))
	}

	rolledBackGID := dtmcli.MustGenGid(dtmServer)
	err := dtmcli.XaGlobalTransaction(dtmServer, rolledBackGID, func(xa *dtmcli.Xa) (*resty.Response, error) {
		response, err := xa.CallBranch(
			map[string]string{"source": "dtm-labs-go", "outcome": "rollback"},
			"http://127.0.0.1:18091/upstream/xa",
		)
		if err != nil {
			return response, err
		}
		return response, dtmcli.ErrFailure
	})
	if !errors.Is(err, dtmcli.ErrFailure) {
		panic(fmt.Errorf("official XA rollback returned %v instead of ErrFailure", err))
	}
	waitStatus(baseURL, token, rolledBackGID, "aborted")
	if rows := xaValueCount(harness.db, rolledBackGID); rows != 0 {
		panic(fmt.Errorf("official XA rollback left %d business rows", rows))
	}
	for name, calls := range map[string]uint64{
		"action":   harness.actionCalls.Load(),
		"commit":   harness.commitCalls.Load(),
		"rollback": harness.rollbackCalls.Load(),
	} {
		if calls == 0 {
			panic(fmt.Errorf("official dtm-labs XA did not execute %s", name))
		}
	}
}

func xaValueCount(db *sql.DB, gid string) int {
	var count int
	must(db.QueryRow("SELECT COUNT(*) FROM roze_dtm_go_xa_values WHERE gid = $1", gid).Scan(&count))
	return count
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
