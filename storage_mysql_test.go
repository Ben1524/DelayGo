package delaygo

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewMySQLStorageFromDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Expect initTables queries
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_meta").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_body").WillReturnResult(sqlmock.NewResult(0, 0))

	storage, err := NewMySQLStorageFromDB(db)
	if err != nil {
		t.Fatalf("NewMySQLStorageFromDB error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	if storage.db == nil {
		t.Error("db should not be nil")
	}

	// 验证默认配置
	if storage.maxBatchSize != 1000 {
		t.Errorf("Expected maxBatchSize=1000, got %d", storage.maxBatchSize)
	}
	if storage.maxBatchBytes != 16*1024*1024 {
		t.Errorf("Expected maxBatchBytes=16MB, got %d", storage.maxBatchBytes)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQLStorageSaveDelayJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Expect initTables queries
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_meta").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_body").WillReturnResult(sqlmock.NewResult(0, 0))

	storage, err := NewMySQLStorageFromDB(db)
	if err != nil {
		t.Fatalf("NewMySQLStorageFromDB error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()
	meta := NewDelayJobMeta(1, "test", 10, 5*time.Second, 30*time.Second)
	body := []byte("test body")

	// Expect SaveDelayJob queries
	// Note: SaveDelayJob uses batchSaveLoop which runs in background.
	// However, for a single item, it might be processed quickly or we might need to wait.
	// But wait, SaveDelayJob sends to a channel. The actual SQL execution happens in batchSaveLoop.
	// This makes testing tricky because it's async.
	// BUT, looking at storage_mysql.go, SaveDelayJob waits for the result:
	// req := &mysqlSaveDelayJobRequest{... done: make(chan error, 1) ...}
	// s.saveChan <- req
	// err := <-req.done
	// So it IS synchronous from the caller's perspective.

	// The batchSaveLoop will execute:
	// INSERT IGNORE INTO delay_job_meta ...
	// INSERT IGNORE INTO delay_job_body ...

	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO delay_job_meta").WithArgs(
		meta.ID, meta.Topic, meta.Priority, meta.DelayState, int64(meta.Delay), int64(meta.TTR),
		meta.CreatedAt.UnixNano(), meta.ReadyAt.UnixNano(),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // ReservedAt, BuriedAt, DeletedAt (can be null)
		meta.Reserves, meta.Timeouts, meta.Releases, meta.Buries, meta.Kicks, meta.Touches, int64(meta.TotalTouchTime),
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT IGNORE INTO delay_job_body").WithArgs(
		meta.ID, body,
	).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Save delayJob
	err = storage.SaveDelayJob(ctx, meta, body)
	if err != nil {
		t.Fatalf("SaveDelayJob error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQLStorageGetDelayJobMeta(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_meta").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_body").WillReturnResult(sqlmock.NewResult(0, 0))

	storage, err := NewMySQLStorageFromDB(db)
	if err != nil {
		t.Fatalf("NewMySQLStorageFromDB error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Expect GetDelayJobMeta query
	// Columns: id, topic, priority, state, delay, ttr, created_at, ready_at, reserved_at, buried_at, deleted_at, reserves, timeouts, releases, buries, kicks, touches, total_touch_time
	rows := sqlmock.NewRows([]string{
		"id", "topic", "priority", "state", "delay", "ttr",
		"created_at", "ready_at", "reserved_at", "buried_at", "deleted_at",
		"reserves", "timeouts", "releases", "buries", "kicks", "touches", "total_touch_time",
	}).AddRow(
		1, "test", 10, DelayStateReady, int64(5*time.Second), int64(30*time.Second),
		time.Now().UnixNano(), time.Now().UnixNano(), nil, nil, nil,
		0, 0, 0, 0, 0, 0, 0,
	)

	mock.ExpectQuery("SELECT id, topic, priority, state, delay, ttr").
		WithArgs(1).
		WillReturnRows(rows)

	gotMeta, err := storage.GetDelayJobMeta(ctx, 1)
	if err != nil {
		t.Fatalf("GetDelayJobMeta error: %v", err)
	}
	if gotMeta.ID != 1 {
		t.Errorf("ID = %d, want 1", gotMeta.ID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQLStorageDeleteDelayJob(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_meta").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_body").WillReturnResult(sqlmock.NewResult(0, 0))

	storage, err := NewMySQLStorageFromDB(db)
	if err != nil {
		t.Fatalf("NewMySQLStorageFromDB error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Expect DeleteDelayJob queries
	mock.ExpectExec("DELETE FROM delay_job_meta WHERE id = ?").WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))

	if err := storage.DeleteDelayJob(ctx, 1); err != nil {
		t.Fatalf("DeleteDelayJob error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMySQLStorageCountDelayJobs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_meta").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS delay_job_body").WillReturnResult(sqlmock.NewResult(0, 0))

	storage, err := NewMySQLStorageFromDB(db)
	if err != nil {
		t.Fatalf("NewMySQLStorageFromDB error: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// 1. Count all
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM delay_job_meta").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	count, err := storage.CountDelayJobs(ctx, nil)
	if err != nil {
		t.Fatalf("CountDelayJobs all error: %v", err)
	}
	if count != 10 {
		t.Errorf("Count all = %d, want 10", count)
	}

	// 2. Count with filter (Ready)
	ready := DelayStateReady
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM delay_job_meta WHERE state = \\?").
		WithArgs(ready).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))

	count, err = storage.CountDelayJobs(ctx, &DelayJobMetaFilter{DelayState: &ready})
	if err != nil {
		t.Fatalf("CountDelayJobs Ready error: %v", err)
	}
	if count != 5 {
		t.Errorf("Count Ready = %d, want 5", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
