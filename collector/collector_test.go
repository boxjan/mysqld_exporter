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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	. "github.com/smartystreets/goconvey/convey"
)

type labelMap map[string]string

type MetricResult struct {
	labels     labelMap
	value      float64
	metricType dto.MetricType
}

func readMetric(m prometheus.Metric) MetricResult {
	pb := &dto.Metric{}
	m.Write(pb)
	labels := make(labelMap, len(pb.Label))
	for _, v := range pb.Label {
		labels[v.GetName()] = v.GetValue()
	}
	if pb.Gauge != nil {
		return MetricResult{labels: labels, value: pb.GetGauge().GetValue(), metricType: dto.MetricType_GAUGE}
	}
	if pb.Counter != nil {
		return MetricResult{labels: labels, value: pb.GetCounter().GetValue(), metricType: dto.MetricType_COUNTER}
	}
	if pb.Untyped != nil {
		return MetricResult{labels: labels, value: pb.GetUntyped().GetValue(), metricType: dto.MetricType_UNTYPED}
	}
	panic("Unsupported metric type")
}

func sanitizeQuery(q string) string {
	q = strings.Join(strings.Fields(q), " ")
	q = strings.Replace(q, "(", "\\(", -1)
	q = strings.Replace(q, ")", "\\)", -1)
	q = strings.Replace(q, "*", "\\*", -1)
	return q
}

func TestParseGTID(t *testing.T) {
	testcase := []struct {
		s      string
		target []GlobalTransactionIdentifier
	}{
		{
			s: "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
			target: []GlobalTransactionIdentifier{
				{
					ServerId:         "3E11FA47-71CA-11E1-9E33-C80AA9429562",
					FirstTransaction: 23,
					LastTransaction:  23,
					Transactions:     []TransactionDetail{{Start: 23, End: 23}},
				},
			},
		},
		{
			s: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
			target: []GlobalTransactionIdentifier{
				{
					ServerId:         "3E11FA47-71CA-11E1-9E33-C80AA9429562",
					FirstTransaction: 1,
					LastTransaction:  5,
					Transactions:     []TransactionDetail{{Start: 1, End: 5}},
				},
			},
		},
		{
			s:      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5-6",
			target: nil,
		},
		{
			s:      "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-A",
			target: nil,
		},
		{
			s:      "3E11FA47-71CA-11E1-9E33-C80AA9429562",
			target: nil,
		},
		{
			s:      "3E11FA47-71CA-11E1-9E33-C80AA9429562:",
			target: nil,
		},
		{
			s: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:11:47-49",
			target: []GlobalTransactionIdentifier{
				{
					ServerId:         "3E11FA47-71CA-11E1-9E33-C80AA9429562",
					FirstTransaction: 1,
					LastTransaction:  49,
					Transactions:     []TransactionDetail{{Start: 1, End: 3}, {Start: 11, End: 11}, {Start: 47, End: 49}},
				},
			},
		},
		{
			s: "2174B383-5441-11E8-B90A-C80AA9429562:1-3, 24DA167-0C0C-11E8-8442-00059A3C7B00:1-19",
			target: []GlobalTransactionIdentifier{
				{
					ServerId:         "2174B383-5441-11E8-B90A-C80AA9429562",
					FirstTransaction: 1,
					LastTransaction:  3,
					Transactions:     []TransactionDetail{{Start: 1, End: 3}},
				},
				{
					ServerId:         "24DA167-0C0C-11E8-8442-00059A3C7B00",
					FirstTransaction: 1,
					LastTransaction:  19,
					Transactions:     []TransactionDetail{{Start: 1, End: 19}},
				},
			},
		},
	}

	Convey("gtid parse", t, func() {
		for _, test := range testcase {
			got, _ := ParseGTID(test.s)
			So(got, ShouldResemble, test.target)
		}
	})
}
