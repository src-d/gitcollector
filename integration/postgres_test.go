package integration

import (
	"database/sql"
	"runtime"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-log.v1"
)

const (
	orgs = "git-fixtures"

	dbTable = "test_table"
)

// TODO https://github.com/src-d/infrastructure/issues/1130
var expMetric = metric{
	org:        "git-fixtures",
	discovered: 8,
	downloaded: 7,
	updated:    0,
	// this failed repo is an empty one
	failed: 1,
}

type metric struct {
	org        string
	discovered int
	downloaded int
	updated    int
	failed     int
}

// TODO maybe we can somehow mock up failure of write metrics query?
func TestPostgres(t *testing.T) {
	// docker service is not supported on osx https://github.com/travis-ci/travis-ci/issues/5738#issuecomment-227154200
	if runtime.GOOS == "darwin" {
		t.Skip("cannot run these tests on osx")
	}

	h, err := NewHelper(orgs)
	require.NoError(t, err)
	defer h.Close()

	for _, tst := range []struct {
		name string
		fnc  func(t *testing.T, h *helper)
	}{
		{"testPostgresEmptyURI", testPostgresEmptyURI},
		{"testBrokenPostgresEndpoint", testBrokenPostgresEndpoint},
		{"testPostgresCreateSchemaFail", testPostgresCreateSchemaFail},
		{"testPostgresSendMetricsSuccess", testPostgresSendMetricsSuccess},
	} {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			defer func() { h.Cleanup() }()
			tst.fnc(t, h)
		})
	}
}

func testPostgresEmptyURI(t *testing.T, h *helper) {
	h.cmd.MetricsDBURI = ""
	require.NoError(t, h.Exec())
}

func testBrokenPostgresEndpoint(t *testing.T, h *helper) {
	h.cmd.MetricsDBURI = "postgres://broken:5432"

	err := h.Exec()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such host")
}

func testPostgresCreateSchemaFail(t *testing.T, h *helper) {
	dbURI, err := h.CreateDB()
	log.Infof("dbURI: %v", dbURI)
	require.NoError(t, err)

	h.cmd.MetricsDBURI = dbURI
	h.cmd.MetricsDBTable = ""

	require.Error(t, h.Exec())
}

func testPostgresSendMetricsSuccess(t *testing.T, h *helper) {
	dbURI, err := h.CreateDB()
	require.NoError(t, err)

	h.cmd.MetricsDBURI = dbURI
	h.cmd.MetricsDBTable = dbTable

	require.NoError(t, h.Exec())

	res, err := parseTable(dbURI, dbTable)
	require.NoError(t, err)
	require.Equal(t, []metric{expMetric}, res)
}

func parseTable(dbURI, table string) (res []metric, err error) {
	cli, err := sql.Open("postgres", dbURI)
	if err != nil {
		return nil, err
	}

	rows, err := cli.Query("select * from " + table)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var m metric
		err = rows.Scan(
			&m.org,
			&m.discovered,
			&m.downloaded,
			&m.updated,
			&m.failed,
		)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return
}
