// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `SHOW MASTER STATUS`.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
)

const (
	// Subsystem
	master = "master_status"
	// Queries.
	masterStatusQueries = `SHOW MASTER STATUS`
)

// Metric descriptors.
var (
	masterBinlogPos = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, master, "binlog_pos"),
		"Combined size of all registered binlog files.",
		[]string{}, nil,
	)
	masterBinlogFileNum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, master, "binlog_file_num"),
		"Number of now use binlog files.",
		[]string{}, nil,
	)
	masterExecutedGtidStart = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, master, "executed_gtid_start"),
		"Number of now use binlog files.",
		[]string{"executed_server_id", "partition"}, nil,
	)
	masterExecutedGtidEnd = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, master, "executed_gtid_end"),
		"Number of now use binlog files.",
		[]string{"executed_server_id", "partition"}, nil,
	)
)

// ScrapeMasterStatus collects from `SHOW MASTER STATUS`.
type ScrapeMasterStatus struct{}

// Name of the Scraper. Should be unique.
func (ScrapeMasterStatus) Name() string {
	return "master_status"
}

// Help describes the role of the Scraper.
func (ScrapeMasterStatus) Help() string {
	return "Collect the master status"
}

// Version of MySQL from which scraper is available.
func (ScrapeMasterStatus) Version() float64 {
	return 5.1
}

func (s ScrapeMasterStatus) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	masterStatusRow, err := db.QueryContext(ctx, masterStatusQueries)
	if err != nil {
		return err
	}
	defer masterStatusRow.Close()

	var (
		filename        string
		position        int64
		binlogDoDb      string
		binlogIgnoreDb  string
		executedGTidSet string
	)

	columns, err := masterStatusRow.Columns()
	if err != nil {
		return err
	}
	columnCount := len(columns)

	for masterStatusRow.Next() {
		switch columnCount {
		case 4:
			if err := masterStatusRow.Scan(&filename, &position, &binlogDoDb, &binlogIgnoreDb); err != nil {
				return err
			}
		case 5:
			if err := masterStatusRow.Scan(&filename, &position, &binlogDoDb, &binlogIgnoreDb, &executedGTidSet); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid number of columns: %q", columnCount)
		}
	}
	// only have file need report
	if filename != "" {
		ss := strings.Split(filename, ".")
		if len(ss) < 2 {
			return fmt.Errorf("split %s by `.` item not enough", filename)
		}
		value, err := strconv.ParseFloat(ss[1], 64)
		if err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			masterBinlogFileNum, prometheus.GaugeValue, value,
		)
		ch <- prometheus.MustNewConstMetric(
			masterBinlogPos, prometheus.GaugeValue, float64(position),
		)
	}

	if executedGTidSet != "" {
		GTIDs, err := ParseGTID(executedGTidSet)
		if err != nil {
			return err
		}

		for _, item := range GTIDs {
			ch <- prometheus.MustNewConstMetric(
				masterExecutedGtidStart, prometheus.GaugeValue, float64(item.FirstTransaction),
				item.ServerId, "")
			ch <- prometheus.MustNewConstMetric(
				masterExecutedGtidEnd, prometheus.GaugeValue, float64(item.LastTransaction),
				item.ServerId, "")
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeMasterStatus{}
