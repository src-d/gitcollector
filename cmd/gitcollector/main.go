package main

import (
	"github.com/src-d/gitcollector/cmd/gitcollector/subcmd"
	"gopkg.in/src-d/go-cli.v0"
)

var (
	version string
	build   string
)

var app = cli.New("gitcollector", version, build, "source{d} tool to download repositories into siva files")

func main() {
	app.AddCommand(&subcmd.DownloadCmd{})
	app.RunMain()
}
