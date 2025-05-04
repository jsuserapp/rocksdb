package rocksdb

/*
#cgo CFLAGS: -I${SRCDIR}/deps/include
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/deps/libs/linux_amd64/librocksdb.a -lm -lstdc++ -lz -lbz2 -lsnappy -llz4 -lzstd
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/deps/libs/windows_amd64 -lrocksdb -lstdc++ -lz -lbz2 -lsnappy -llz4 -lzstd -lshlwapi -lrpcrt4

#include <stdlib.h>
#include <string.h>
#include "c.h"
*/
import "C"
import (
	"fmt"
	"github.com/jsuserapp/ju"
	"time"
	"unsafe"
)

const (
	testDbPath  = "./tmp/testdb"
	testBackup1 = "./tmp/backup1"
	testNewDb   = "./tmp/testnewdb"
	testResore1 = "./tmp/restore1"
)

func TestBackup() {
	opts := GetDefaultOptions()
	db, err := Open(testDbPath, opts)
	if ju.CheckFailure(err) {
		return
	}
	defer db.Close()

	//db.AddColumnFamily([]string{"tab2"}, opts)

	ju.LogBlue(db.ListColumnFamily())

	//cf := db.GetColumnFamily("tab1")
	//putData(cf)
	//listCf(cf)
	var be BackupEngine
	if !be.Open(testBackup1, nil) {
		return
	}
	defer be.Close()

	//be.CreateBackup(rdb)
	be.GetInfo()

	//if !be.Restore(testNewDb, 1) {
	//	return
	//}

	newDb, err := Open(testNewDb, nil)
	if ju.CheckFailure(err) {
		return
	}
	defer newDb.Close()

	cf := newDb.GetColumnFamily("tab1")
	if cf == nil {
		return
	}
	listCf(cf)
}
func listCf(cf *ColumnFamily) {
	cf.ListPrefix(nil, func(key, val []byte) bool {
		ju.LogGreen(string(key), "=", string(val))
		return true
	})
}
func putData(cf *ColumnFamily) {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("2%d", i)
		val := fmt.Sprintf("v%d", i)
		err := cf.Put([]byte(key), []byte(val))
		if ju.CheckFailure(err) {
			return
		}
	}
}
func Restore() {
	//RestoreBackup(testNewDb, testBackup1, 1)
	//checkDb(testNewDb)
	var be BackupEngine
	if !be.Open(testBackup1, nil) {
		return
	}
	defer be.Close()
	be.GetInfo()
}
func checkDb(dbPath string) {
	db, e := Open(dbPath, nil)
	if ju.CheckFailure(e) {
		return
	}
	defer func() {
		db.Close()
	}()
	cfs := db.ListColumnFamily()
	ju.LogGreen(cfs)
	cf := db.GetColumnFamily("tab1")
	if cf == nil {
		return
	}
	cf.ListPrefix(nil, func(key, val []byte) bool {
		ju.LogGreen(string(key), "=", string(val))
		return true
	})
}
func backup() {
	//rdb.CreateBackup(testBackup1, 1)
}

type BackupEngine struct {
	engine *C.rocksdb_backup_engine_t
}

func (be *BackupEngine) Close() {
	if be.engine != nil {
		C.rocksdb_backup_engine_close(be.engine)
		be.engine = nil
	}
}
func (be *BackupEngine) Open(backupPath string, opts *Options) bool {
	if be.engine != nil {
		return true
	}
	cBack := C.CString(backupPath)
	var engine *C.rocksdb_backup_engine_t
	if opts == nil {
		opts = GetDefaultOptions()
		defer opts.Close()
	}
	var err *C.char
	//rocksdb_backup_engine_t* rocksdb_backup_engine_open(const rocksdb_options_t* options, const char* path, char** errptr);
	engine = C.rocksdb_backup_engine_open(opts.handle, cBack, &err)
	C.free(unsafe.Pointer(cBack))
	if err != nil {
		ju.LogRed(charErr(err).Error())
		return false
	}
	be.engine = engine
	return true
}
func (be *BackupEngine) Verify(backupId int) bool {
	var err *C.char
	id := C.uint32_t(backupId)
	//void rocksdb_backup_engine_verify_backup(rocksdb_backup_engine_t* be, uint32_t backup_id, char** errptr)
	C.rocksdb_backup_engine_verify_backup(be.engine, id, &err)
	if err != nil {
		ju.LogRed(charErr(err).Error())
		return false
	}
	return true
}

// CreateBackup 即使没有新的数据，这个函数也会创建一个新的备份点，但是它的内容和上一次的备份点是相同的
func (be *BackupEngine) CreateBackup(db *Db) bool {
	var err *C.char
	//void rocksdb_backup_engine_create_new_backup(rocksdb_backup_engine_t* be, rocksdb_t* rdb, char** errptr);
	//C.rocksdb_backup_engine_create_new_backup(be.engine, rdb.rdb.rdb, &err)
	//void rocksdb_backup_engine_create_new_backup_flush(rocksdb_backup_engine_t* be, rocksdb_t* rdb,unsigned char flush_before_backup, char** errptr);
	C.rocksdb_backup_engine_create_new_backup_flush(be.engine, db.GetDefault().rocks.db, 1, &err)
	if err != nil {
		ju.LogRed(charErr(err).Error())
		return false
	}
	return true
}
func (be *BackupEngine) GetInfo() {
	//const rocksdb_backup_engine_info_t* rocksdb_backup_engine_get_backup_info(rocksdb_backup_engine_t* be);
	var backupInfo *C.rocksdb_backup_engine_info_t
	backupInfo = C.rocksdb_backup_engine_get_backup_info(be.engine)
	if backupInfo == nil {
		ju.LogRed("get backup into return nil")
		return
	}

	//int rocksdb_backup_engine_info_count(const rocksdb_backup_engine_info_t* info);
	count := C.rocksdb_backup_engine_info_count(backupInfo)
	var i C.int
	for i = 0; i < count; i++ {
		//int64_t rocksdb_backup_engine_info_timestamp(const rocksdb_backup_engine_info_t* info, int index);
		ts := C.rocksdb_backup_engine_info_timestamp(backupInfo, i)
		timeStamp := int64(ts)
		//uint32_t rocksdb_backup_engine_info_backup_id(const rocksdb_backup_engine_info_t* info, int index);
		id := uint32(C.rocksdb_backup_engine_info_backup_id(backupInfo, i))
		//uint64_t rocksdb_backup_engine_info_size(const rocksdb_backup_engine_info_t* info, int index);
		size := uint64(C.rocksdb_backup_engine_info_size(backupInfo, i))

		tm := time.Unix(timeStamp, 0).Format("2006-01-02 15:04:05.000")
		ju.LogMagentaF("backup info: index=%d,id=%d,time=%s,size:%d\n", i, id, tm, size)
	}
	//void rocksdb_backup_engine_info_destroy(const rocksdb_backup_engine_info_t* info);
	C.rocksdb_backup_engine_info_destroy(backupInfo)
}
func (be *BackupEngine) Restore(restorePath string, backupId int) bool {
	restoreOpts := C.rocksdb_restore_options_create()
	cDb := C.CString(restorePath)
	var err *C.char
	if backupId < 0 {
		//void rocksdb_backup_engine_restore_db_from_latest_backup(rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
		//    const rocksdb_restore_options_t* restore_options, char** errptr);
		C.rocksdb_backup_engine_restore_db_from_latest_backup(be.engine, cDb, cDb, restoreOpts, &err)
	} else {
		id := C.uint32_t(backupId)
		//void rocksdb_backup_engine_restore_db_from_backup(
		//    rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
		//    const rocksdb_restore_options_t* restore_options, const uint32_t backup_id,
		//    char** errptr);
		C.rocksdb_backup_engine_restore_db_from_backup(be.engine, cDb, cDb, restoreOpts, id, &err)
	}
	C.free(unsafe.Pointer(cDb))
	C.rocksdb_restore_options_destroy(restoreOpts)
	if err != nil {
		ju.LogRed(charErr(err).Error())
		return false
	}
	return true
}
func RestoreBackup(dbPath, backupPath string, backupId int) bool {
	cBack := C.CString(backupPath)
	var engine *C.rocksdb_backup_engine_t
	opts := GetDefaultOptions()
	defer opts.Close()
	var err *C.char
	//rocksdb_backup_engine_t* rocksdb_backup_engine_open(const rocksdb_options_t* options, const char* path, char** errptr);
	engine = C.rocksdb_backup_engine_open(opts.handle, cBack, &err)
	C.free(unsafe.Pointer(cBack))
	if err != nil {
		ju.LogRed(charErr(err).Error())
		return false
	}
	defer func() {
		if engine != nil {
			C.rocksdb_backup_engine_close(engine)
			engine = nil
		}
	}()

	//rocksdb_restore_options_t* rocksdb_restore_options_create(void);
	//void rocksdb_restore_options_destroy(rocksdb_restore_options_t* opt);
	//void rocksdb_restore_options_set_keep_log_files(rocksdb_restore_options_t* opt, int v);

	//s = backup_engine_ro->RestoreDBFromBackup(1, "/tmp/rocksdb_example",
	//"/tmp/rocksdb_example");
	//assert(s.ok());
	//void rocksdb_backup_engine_restore_db_from_latest_backup(
	//    rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
	//    const rocksdb_restore_options_t* restore_options, char** errptr);
	//
	//void rocksdb_backup_engine_restore_db_from_backup(
	//    rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
	//    const rocksdb_restore_options_t* restore_options, const uint32_t backup_id,
	//    char** errptr);
	restoreOpts := C.rocksdb_restore_options_create()
	cDb := C.CString(dbPath)
	id := C.uint32_t(backupId)
	C.rocksdb_backup_engine_restore_db_from_backup(engine, cDb, cDb, restoreOpts, id, &err)
	C.free(unsafe.Pointer(cDb))
	C.rocksdb_restore_options_destroy(restoreOpts)
	if err != nil {
		ju.LogRed(charErr(err).Error())
		return false
	}
	return true
}

//func main() {
//DB* rdb;
//Options options;
//// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
//options.IncreaseParallelism();
//options.OptimizeLevelStyleCompaction();
//// create the DB if it's not already present
//options.create_if_missing = true;
//
//// open DB
//Status s = DB::Open(options, kDBPath, &rdb);
//assert(s.ok());
//
//// Put key-value
//rdb->Put(WriteOptions(), "key1", "value1");
//assert(s.ok());
//
//// create backup
//BackupEngine* backup_engine;
//s = BackupEngine::Open(Env::Default(),
//BackupEngineOptions("/tmp/rocksdb_example_backup"),
//&backup_engine);
//assert(s.ok());
//
//backup_engine->CreateNewBackup(rdb);
//assert(s.ok());
//
//std::vector<BackupInfo> backup_info;
//backup_engine->GetBackupInfo(&backup_info);
//
//s = backup_engine->VerifyBackup(1);
//assert(s.ok());
//
//// Put key-value
//rdb->Put(WriteOptions(), "key2", "value2");
//assert(s.ok());
//
//rdb->Close();
//delete rdb;
//rdb = nullptr;
//
//// restore rdb to backup 1
//BackupEngineReadOnly* backup_engine_ro;
//s = BackupEngineReadOnly::Open(
//Env::Default(), BackupEngineOptions("/tmp/rocksdb_example_backup"),
//&backup_engine_ro);
//assert(s.ok());
//
//s = backup_engine_ro->RestoreDBFromBackup(1, "/tmp/rocksdb_example",
//"/tmp/rocksdb_example");
//assert(s.ok());
//
//// open rdb again
//s = DB::Open(options, kDBPath, &rdb);
//assert(s.ok());
//
//std::string value;
//s = rdb->Get(ReadOptions(), "key1", &value);
//assert(!s.IsNotFound());
//
//s = rdb->Get(ReadOptions(), "key2", &value);
//assert(s.IsNotFound());
//
//delete backup_engine;
//delete backup_engine_ro;
//delete rdb;
//
//return 0;
//}
