package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"gopkg.in/alecthomas/kingpin.v2"
)

const defaultDBPath = "data/"

var (
	blockID      string
	dbPath       string
	deleteSource bool
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	if err := run(logger); err != nil {
		level.Error(logger).Log("msg", "failed to run downsample", "error", err)
		os.Exit(1)
	}
}

func run(logger log.Logger) error {
	app := kingpin.New(filepath.Base(os.Args[0]), "A tool to downsample Prometheus or Thanos TSDB block")
	app.HelpFlag.Short('h')

	app.Flag("id", "Block ID to downsample").StringVar(&blockID)
	app.Flag("delete-source-block", "Whether to delete source block or not after downsampling. Default is false.").Default("false").BoolVar(&deleteSource)
	app.Arg("db path", "Database path (default is "+defaultDBPath+").").Default(defaultDBPath).StringVar(&dbPath)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	blockDir := filepath.Join(dbPath, blockID)
	meta, err := metadata.ReadFromDir(blockDir)
	if err != nil {
		return err
	}
	ctx := context.Background()
	pool := chunkenc.NewPool()
	block, err := tsdb.OpenBlock(logger, blockDir, pool)
	if err != nil {
		return err
	}

	begin := time.Now()
	id, err := downsample.Downsample(ctx, logger, meta, block, dbPath, downsample.ResLevel1)
	if err != nil {
		return err
	}
	downsampleDuration := time.Since(begin)
	level.Info(logger).Log("msg", "downsampled block",
		"from", blockID, "to", id, "duration", downsampleDuration, "duration_ms", downsampleDuration.Milliseconds())

	if deleteSource {
		if err := os.RemoveAll(blockDir); err != nil {
			return errors.Wrapf(err, "delete source block after relabeling:%s", id)
		}
	}
	return nil
}
