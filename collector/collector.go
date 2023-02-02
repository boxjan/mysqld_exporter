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

package collector

import (
	"bytes"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Exporter namespace.
	namespace = "mysql"
	// Math constant for picoseconds to seconds.
	picoSeconds = 1e12
	// Query to check whether user/table/client stats are enabled.
	userstatCheckQuery = `SHOW GLOBAL VARIABLES WHERE Variable_Name='userstat'
		OR Variable_Name='userstat_running'`
)

type TransactionDetail struct {
	Start, End int64
}

type GlobalTransactionIdentifier struct {
	ServerId                          string
	FirstTransaction, LastTransaction int64
	Transactions                      []TransactionDetail
}

var logRE = regexp.MustCompile(`.+\.(\d+)$`)

func newDesc(subsystem, name, help string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help, nil, nil,
	)
}

func parseStatus(data sql.RawBytes) (float64, bool) {
	dataString := strings.ToLower(string(data))
	switch dataString {
	case "yes", "on":
		return 1, true
	case "no", "off", "disabled":
		return 0, true
	// SHOW SLAVE STATUS Slave_IO_Running can return "Connecting" which is a non-running state.
	case "connecting":
		return 0, true
	// SHOW GLOBAL STATUS like 'wsrep_cluster_status' can return "Primary" or "non-Primary"/"Disconnected"
	case "primary":
		return 1, true
	case "non-primary", "disconnected":
		return 0, true
	}
	if ts, err := time.Parse("Jan 02 15:04:05 2006 MST", string(data)); err == nil {
		return float64(ts.Unix()), true
	}
	if ts, err := time.Parse("2006-01-02 15:04:05", string(data)); err == nil {
		return float64(ts.Unix()), true
	}
	if logNum := logRE.Find(data); logNum != nil {
		value, err := strconv.ParseFloat(string(logNum), 64)
		return value, err == nil
	}
	value, err := strconv.ParseFloat(string(data), 64)
	return value, err == nil
}

func parsePrivilege(data sql.RawBytes) (float64, bool) {
	if bytes.Equal(data, []byte("Y")) {
		return 1, true
	}
	if bytes.Equal(data, []byte("N")) {
		return 0, true
	}
	return -1, false
}

func ParseGTID(s string) ([]GlobalTransactionIdentifier, error) {
	var res []GlobalTransactionIdentifier
	gtidItems := strings.Split(s, ",")

	for _, item := range gtidItems {
		item = strings.TrimSpace(item)
		g := GlobalTransactionIdentifier{}
		ss := strings.Split(item, ":")
		if len(ss) < 2 {
			return nil, fmt.Errorf("can not parse gtid: %s, transaction item is too little", item)
		}

		g.ServerId = ss[0]
		t := TransactionDetail{}
		var err error
		for i := 1; i < len(ss); i++ {
			sss := strings.Split(ss[i], "-")
			if len(sss) > 2 {
				return nil, fmt.Errorf("can not part gtid: %s, cut by '-' more than 2 item", item)
			}
			t.Start, err = strconv.ParseInt(sss[0], 10, 0)
			if err != nil {
				return nil, fmt.Errorf("parse %s to int64 failed with err: %+v", sss[0], err)
			}
			if len(sss) == 1 {
				t.End = t.Start
			} else {
				t.End, err = strconv.ParseInt(sss[1], 10, 0)
				if err != nil {
					return nil, fmt.Errorf("parse %s to int64 failed with err: %+v", sss[0], err)
				}
			}
			g.LastTransaction = t.End
			g.Transactions = append(g.Transactions, t)
		}
		g.FirstTransaction = g.Transactions[0].Start
		res = append(res, g)
	}

	return res, nil
}
