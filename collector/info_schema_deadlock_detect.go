package collector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"time"
)

const infoSchemaInnodbLockWaitsQuery = `SELECT REQUESTING_TRX_ID, REQUESTED_LOCK_ID, BLOCKING_TRX_ID, BLOCKING_LOCK_ID FROM information_schema.innodb_lock_waits`

const performanceSchemaDataLockWaitsQuery = `SELECT REQUESTING_ENGINE_TRANSACTION_ID, REQUESTING_ENGINE_LOCK_ID, BLOCKING_ENGINE_TRANSACTION_ID, BLOCKING_ENGINE_LOCK_ID FROM performance_schema.data_lock_waits`

const infoSchemaInnodbLocksQuery = `SELECT lock_trx_id, lock_mode, lock_type, lock_table, lock_index FROM information_schema.innodb_locks WHERE lock_id IN (%s)`

const performanceSchemaDataLocksQuery = `SELECT ENGINE_TRANSACTION_ID, LOCK_MODE, LOCK_TYPE, OBJECT_NAME, INDEX_NAME FROM performance_schema.data_locks WHERE ENGINE_LOCK_ID IN (%s)`

const infoSchemaInnodbTrxQuery = `SELECT trx_mysql_thread_id, trx_id, trx_requested_lock_id, trx_started, trx_wait_started, trx_state, trx_query FROM information_schema.innodb_trx WHERE trx_id IN (%s)`

const infoSchemaShowProcesslist = `
		  SELECT
			ID,
		    USER,
		    DB
		  FROM information_schema.processlist
		  WHERE ID IN (%s)
	`
const mysqlVersionQuery = `SHOW GLOBAL VARIABLES like 'version'`

var deadlockInfoDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, informationSchema, "deadlock_info"),
	"The related information of deadlock.",
	[]string{"ts", "thread", "trx_id", "txd_time", "user", "db", "tbl", "idx", "lock_type", "lock_mode", "wait_hold", "query"}, nil)

type ScrapeDeadlockInfo struct{}

func (ScrapeDeadlockInfo) Version() float64 {
	return 5.6
}

func (ScrapeDeadlockInfo) Name() string {
	return informationSchema + ".deadlock_info"
}

func (ScrapeDeadlockInfo) Help() string {
	return "Collect current deadlock information"
}

type void struct{}

var member void

type DeadlockInfo struct {
	thread         int
	trxId          string
	lockId         string
	trxStarted     string
	trxWaitStarted string
	user           string
	db             string
	tbl            string
	idx            string
	lockType       string
	lockMode       string
	waitHold       string
	query          string
}

func (ScrapeDeadlockInfo) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	versionRow := db.QueryRowContext(ctx, mysqlVersionQuery)
	var mysqlVersion string
	var variableName string
	if err := versionRow.Scan(&variableName, &mysqlVersion); err != nil {
		return err
	}
	var (
		lockWaitsQuery string
		locksQuery     string
	)
	if mysqlVersion[0] == 8 {
		lockWaitsQuery = performanceSchemaDataLockWaitsQuery
		locksQuery = performanceSchemaDataLocksQuery
	} else {
		lockWaitsQuery = infoSchemaInnodbLockWaitsQuery
		locksQuery = infoSchemaInnodbLocksQuery
	}

	lockWaitsRows, err := db.QueryContext(ctx, lockWaitsQuery)
	if err != nil {
		return err
	}
	defer lockWaitsRows.Close()

	var (
		requestingTrxId string
		requestedLockId string
		blockingTrxId   string
		blockingLockId  string
	)
	lockWaitsMap := make(map[string]map[string]void)

	for lockWaitsRows.Next() {
		err = lockWaitsRows.Scan(&requestingTrxId, &requestedLockId, &blockingTrxId, &blockingLockId)
		if err != nil {
			return err
		}
		if _, ok := lockWaitsMap[requestingTrxId]; !ok {
			lockWaitsMap[requestingTrxId] = make(map[string]void)
		}
		lockWaitsMap[requestingTrxId][blockingTrxId] = member
	}

	deadlockTrxIds := make(map[string]void)
	trxIdString := ""
	for k, v := range lockWaitsMap {
		if _, ok := deadlockTrxIds[k]; ok {
			continue
		}
		for s := range v {
			if m, ok := lockWaitsMap[s]; ok {
				if _, ok := m[k]; ok {
					deadlockTrxIds[k] = member
					deadlockTrxIds[s] = member
					if len(trxIdString) == 0 {
						trxIdString += fmt.Sprintf("'%s'", k)
						trxIdString += fmt.Sprintf(",'%s'", s)
					} else {
						trxIdString += fmt.Sprintf(",'%s'", k)
						trxIdString += fmt.Sprintf(",'%s'", s)
					}
				}
			}
		}
	}

	if len(deadlockTrxIds) == 0 {
		return nil
	}
	innodbTrxQuery := fmt.Sprintf(infoSchemaInnodbTrxQuery, trxIdString)

	innodbTrxRows, err := db.QueryContext(ctx, innodbTrxQuery)
	if err != nil {
		return err
	}
	deadlockInfoMap := make(map[string]DeadlockInfo)
	threadTrxIdMap := make(map[int]string)
	lockIdString := ""
	threadIdString := ""
	defer innodbTrxRows.Close()
	for innodbTrxRows.Next() {
		deadlockInfo := new(DeadlockInfo)
		err = innodbTrxRows.Scan(&deadlockInfo.thread, &deadlockInfo.trxId, &deadlockInfo.lockId, &deadlockInfo.trxStarted, &deadlockInfo.trxWaitStarted, &deadlockInfo.waitHold, &deadlockInfo.query)
		if err != nil {
			return err
		}
		deadlockInfoMap[deadlockInfo.trxId] = *deadlockInfo
		threadTrxIdMap[deadlockInfo.thread] = deadlockInfo.trxId
		if len(lockIdString) == 0 {
			lockIdString += fmt.Sprintf("'%s'", deadlockInfo.lockId)
			threadIdString += fmt.Sprintf("'%d'", deadlockInfo.thread)
		} else {
			lockIdString += fmt.Sprintf(",'%s'", deadlockInfo.lockId)
			threadIdString += fmt.Sprintf(",'%d'", deadlockInfo.thread)
		}
	}

	innodbLocksQuery := fmt.Sprintf(locksQuery, lockIdString)
	innodbLocksRows, err := db.QueryContext(ctx, innodbLocksQuery)
	if err != nil {
		return err
	}
	defer innodbLocksRows.Close()

	for innodbLocksRows.Next() {
		var (
			trxId     string
			lockMode  string
			lockType  string
			lockTable string
			lockIndex string
		)
		err = innodbLocksRows.Scan(&trxId, &lockMode, &lockType, &lockTable, &lockIndex)
		if err != nil {
			return err
		}
		info := deadlockInfoMap[trxId]
		info.lockMode = lockMode
		info.lockType = lockType
		info.tbl = lockTable
		info.idx = lockIndex
		deadlockInfoMap[trxId] = info
	}

	showProcessListQuery := fmt.Sprintf(infoSchemaShowProcesslist, threadIdString)
	processlistRows, err := db.QueryContext(ctx, showProcessListQuery)
	if err != nil {
		return err
	}
	defer processlistRows.Close()

	for processlistRows.Next() {
		var (
			threadId int
			user     string
			db       string
		)
		err := processlistRows.Scan(&threadId, &user, &db)
		if err != nil {
			return err
		}
		info := deadlockInfoMap[threadTrxIdMap[threadId]]
		info.user = user
		info.db = db
		deadlockInfoMap[threadTrxIdMap[threadId]] = info
	}

	for _, info := range deadlockInfoMap {
		layout := "2006-01-02 15:04:05"
		startTime, _ := time.Parse(layout, info.trxStarted)
		waitStartTime, _ := time.Parse(layout, info.trxWaitStarted)
		duration := waitStartTime.Sub(startTime)
		ch <- prometheus.MustNewConstMetric(deadlockInfoDesc, prometheus.GaugeValue, 1, info.trxWaitStarted,
			strconv.Itoa(info.thread), info.trxId, duration.String(), info.user, info.db, info.tbl, info.idx, info.lockType, info.lockMode, info.waitHold, info.query)
	}
	return nil
}

var _ Scraper = ScrapeDeadlockInfo{}
