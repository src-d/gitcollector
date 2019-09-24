package integration

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/src-d/gitcollector/cmd/gitcollector/subcmd"

	"github.com/ory/dockertest"
)

const (
	dockerImage   = "postgres"
	dockerVersion = "9.6"
)

type helper struct {
	address string
	cmd     subcmd.DownloadCmd
	cli     *sql.DB
	closers []func()
}

func NewHelper(orgs string) (*helper, error) {
	lib, err := ioutil.TempDir("", "gcol-lib")
	if err != nil {
		removePaths(lib)
		return nil, err
	}

	tmp, err := ioutil.TempDir("", "gcol-tmp")
	if err != nil {
		removePaths(tmp, lib)
		return nil, err
	}

	addr, contClose, err := preparePostgres()
	if err != nil {
		return nil, err
	}

	cli, err := sql.Open("postgres", getURI(addr))
	if err != nil {
		contClose()
		return nil, err
	}

	return &helper{
		address: addr,
		cmd: subcmd.DownloadCmd{
			Orgs:    orgs,
			LibPath: lib,
			TmpPath: tmp,
			Token:   os.Getenv("GITHUB_TOKEN"),
		},
		cli: cli,
		closers: []func(){
			func() {
				cli.Close()
				contClose()
			},
			contClose,
		},
	}, nil
}

func (h *helper) Exec() error { return h.cmd.Execute(nil) }

func (h *helper) CreateDB() (string, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	dbTestName := fmt.Sprintf("testing%d", r.Uint64())
	_, err := h.cli.Exec("create database " + dbTestName)
	if err != nil {
		return "", err
	}

	return h.getDBURI(dbTestName), nil
}

func (h *helper) getDBURI(dnName string) string {
	//"postgres://testing:testing@0.0.0.0:5432/%s?sslmode=disable&connect_timeout=10"
	return "postgres://postgres:postgres@" + h.address + "/" + dnName + "?sslmode=disable&connect_timeout=10"
}

func (h *helper) Cleanup() {
	removeDirsContents(h.cmd.TmpPath, h.cmd.LibPath)
}

func (h *helper) Close() {
	for _, c := range h.closers {
		c()
	}
	removePaths(h.cmd.TmpPath, h.cmd.LibPath)
}

// preparePostgres runs postgres container and waits until endpoint is accessible
// returns endpoint's hostport, container close method and error
func preparePostgres() (string, func(), error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", func() {}, err
	}

	cont, err := pool.Run(dockerImage, dockerVersion, []string{
		"POSTGRES_PASSWORD=postgres",
	})
	if err != nil {
		return "", func() {}, err
	}

	const port = "5432/tcp"
	addr := cont.GetHostPort(port)
	if err := pool.Retry(func() error {
		cli, err := sql.Open("postgres", getURI(addr))
		if err != nil {
			return err
		}
		defer cli.Close()
		return cli.Ping()
	}); err != nil {
		cont.Close()
		return "", func() {}, err
	}

	return addr, func() {
		cont.Close()
	}, nil
}

func getURI(hostPort string) string {
	return "postgres://postgres:postgres@" + hostPort + "?sslmode=disable&connect_timeout=10"
}

func removePaths(paths ...string) {
	for _, p := range paths {
		os.RemoveAll(p)
	}
}

func removeDirsContents(dirs ...string) {
	for _, d := range dirs {
		removeContents(d)
	}
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
