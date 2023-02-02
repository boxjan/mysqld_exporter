package collector

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestScrapeMasterStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow("binlog.000006", "49066", "", "", "215d19f8-7eca-11ed-9d98-00163e000147:1-261530")
	mock.ExpectQuery(sanitizeQuery("SHOW MASTER STATUS")).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = (ScrapeMasterStatus{}).Scrape(context.Background(), db, ch, log.NewNopLogger()); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	counterExpected := []MetricResult{
		{labels: labelMap{}, value: 6, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 49066, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"executed_server_id": "215d19f8-7eca-11ed-9d98-00163e000147", "partition": ""}, value: 1, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"executed_server_id": "215d19f8-7eca-11ed-9d98-00163e000147", "partition": ""}, value: 261530, metricType: dto.MetricType_GAUGE},
	}

	Convey("Metrics comparison", t, func() {
		for _, expect := range counterExpected {
			got := readMetric(<-ch)
			So(got, ShouldResemble, expect)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
