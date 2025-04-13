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
	"bytes"
	"errors"
	"unsafe"
)

var errKeyIsNil = errors.New("key Cann't be nil")
var errProcIsNil = errors.New("call back function can not be nil")
var errHandleIsNil = errors.New("handle is nil, it has closed or not inited")

var _gEmpty [1]byte
var gNullPtr = (*C.char)(unsafe.Pointer(&_gEmpty[0]))

func toCBytes(data []byte) (*C.char, C.size_t) {
	if len(data) > 0 {
		return (*C.char)(unsafe.Pointer(&data[0])), C.size_t(len(data))
	}
	return gNullPtr, 0
}
func charErr(err *C.char) error {
	if err == nil {
		return nil
	}
	errStr := C.GoString(err)
	C.free(unsafe.Pointer(err))
	return errors.New(errStr)
}

// Options 定义 RocksDB 的数据库打开选项
type Options struct {
	// CreateIfMissing 如果数据库不存在，是否创建新数据库
	// 默认值: true
	// 设为 false 时，若数据库不存在，打开会失败
	CreateIfMissing bool

	// IncreaseParallelism 增加后台线程的并行度，提升压缩和刷写性能
	// 默认值: 0
	// 通常设置为 CPU 核心数或稍低值，0 表示不调整
	IncreaseParallelism int

	// ErrorIfExists 如果数据库已存在，是否报错
	// 默认值: false
	// 设为 true 时，若数据库已存在，打开会失败
	ErrorIfExists bool

	// WriteBufferSize MemTable 的大小（字节），影响内存使用和写性能
	// 默认值: 0
	// 增大可减少刷盘频率，但占用更多内存
	WriteBufferSize int

	// MaxOpenFiles 最大打开文件数，影响文件句柄使用
	// 默认值: 0
	// 设为 -1 表示无限制，小值可能导致性能下降
	MaxOpenFiles int

	// DisableWAL 是否禁用 Write-Ahead Log（WAL）
	// 默认值: false
	// 设为 true 时，数据仅存内存，程序退出后丢失，适合临时数据库
	DisableWAL bool

	// CompressionType 数据压缩类型，影响存储空间和读写性能
	// 默认值: "snappy" (支持: "none", "snappy", "zlib", "bzip2", "lz4", "zstd")
	// "none" 表示无压缩，"snappy" 平衡速度和压缩率
	CompressionType string

	// TargetFileSizeBase 每个 SST 文件的目标大小（字节）
	// 默认值: 0
	// 影响压缩和读取性能，小值增加文件数，大值减少文件数
	TargetFileSizeBase int

	// MaxBackgroundJobs 后台任务（如压缩、刷盘）的最大线程数
	// 默认值: 0
	// 增大可提升后台处理速度，但消耗更多 CPU
	MaxBackgroundJobs int

	// AllowConcurrentMemtableWrite 是否允许多线程并发写入 MemTable
	// 默认值: false (RocksDB 6.7+ 支持)
	// 设为 true 可提升多线程写性能
	AllowConcurrentMemtableWrite bool
}

// GetDefaultOptions 返回默认的 RocksDB 选项
func GetDefaultOptions() *Options {
	return &Options{
		CreateIfMissing:              true, // 如果数据库不存在，自动创建
		IncreaseParallelism:          0,
		ErrorIfExists:                false, // 允许覆盖现有数据库
		WriteBufferSize:              0,
		MaxOpenFiles:                 0,
		DisableWAL:                   false,    // 默认启用 WAL
		CompressionType:              "snappy", // 使用 Snappy 压缩
		TargetFileSizeBase:           0,
		MaxBackgroundJobs:            0,     // 后台任务线程
		AllowConcurrentMemtableWrite: false, // 默认禁用并发 MemTable 写
	}
}

// ApplyOptions 将 Go 的 Options 应用到 RocksDB 的 C 选项
func applyOptions(opts *C.rocksdb_options_t, options *Options) {
	C.rocksdb_options_set_create_if_missing(opts, C.uchar(boolToInt(options.CreateIfMissing)))
	if options.IncreaseParallelism != 0 {
		C.rocksdb_options_increase_parallelism(opts, C.int(options.IncreaseParallelism))
	}
	C.rocksdb_options_set_error_if_exists(opts, C.uchar(boolToInt(options.ErrorIfExists)))
	if options.WriteBufferSize != 0 {
		C.rocksdb_options_set_write_buffer_size(opts, C.size_t(options.WriteBufferSize))
	}
	if options.MaxOpenFiles != 0 {
		C.rocksdb_options_set_max_open_files(opts, C.int(options.MaxOpenFiles))
	}

	switch options.CompressionType {
	case "none":
		C.rocksdb_options_set_compression(opts, C.rocksdb_no_compression)
	case "snappy":
		C.rocksdb_options_set_compression(opts, C.rocksdb_snappy_compression)
	case "zlib":
		C.rocksdb_options_set_compression(opts, C.rocksdb_zlib_compression)
	case "bzip2":
		C.rocksdb_options_set_compression(opts, C.rocksdb_bz2_compression)
	case "lz4":
		C.rocksdb_options_set_compression(opts, C.rocksdb_lz4_compression)
	case "zstd":
		C.rocksdb_options_set_compression(opts, C.rocksdb_zstd_compression)
	default:
		C.rocksdb_options_set_compression(opts, C.rocksdb_snappy_compression) // 默认 Snappy
	}

	if options.TargetFileSizeBase != 0 {
		C.rocksdb_options_set_target_file_size_base(opts, C.uint64_t(options.TargetFileSizeBase))
	}
	if options.MaxBackgroundJobs != 0 {
		C.rocksdb_options_set_max_background_jobs(opts, C.int(options.MaxBackgroundJobs))
	}
	C.rocksdb_options_set_allow_concurrent_memtable_write(opts, C.uchar(boolToInt(options.AllowConcurrentMemtableWrite)))
}

// boolToInt 将 Go 的 bool 转换为 C 的 int
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

type RocksDb struct {
	db *C.rocksdb_t
	wo *C.rocksdb_writeoptions_t
	ro *C.rocksdb_readoptions_t
}

func Open(path string, options *Options) (*RocksDb, error) {
	db := &RocksDb{}
	if options == nil {
		options = GetDefaultOptions()
	}
	opts := C.rocksdb_options_create()
	applyOptions(opts, options)
	var err *C.char
	dbPath := C.CString(path)
	_db := C.rocksdb_open(opts, dbPath, &err)
	C.free(unsafe.Pointer(dbPath))
	C.rocksdb_options_destroy(opts)
	if err != nil {
		return nil, charErr(err)
	}
	db.db = _db
	db.wo = C.rocksdb_writeoptions_create()
	if options.DisableWAL {
		C.rocksdb_writeoptions_disable_WAL(db.wo, C.int(boolToInt(options.DisableWAL)))
	}
	db.ro = C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_prefix_same_as_start(db.ro, 1)
	return db, nil
}
func (db *RocksDb) Close() {
	if db.wo != nil {
		C.rocksdb_writeoptions_destroy(db.wo)
		db.wo = nil
	}
	if db.ro != nil {
		C.rocksdb_readoptions_destroy(db.ro)
		db.ro = nil
	}
	if db.db != nil {
		C.rocksdb_close(db.db)
		db.db = nil
	}
}
func (db *RocksDb) Put(key, value []byte) error {
	if db.db == nil {
		return errHandleIsNil
	}
	var err *C.char
	cKey, keyLen := toCBytes(key)
	cValue, valLen := toCBytes(value)
	C.rocksdb_put(db.db, db.wo, cKey, keyLen, cValue, valLen, &err)
	return charErr(err)
}
func (db *RocksDb) Get(key []byte) ([]byte, error) {
	if db.db == nil {
		return nil, errHandleIsNil
	}
	cKey, keyLen := toCBytes(key)
	var err *C.char
	var valLen C.size_t
	value := C.rocksdb_get(db.db, db.ro, cKey, keyLen, &valLen, &err)
	if err != nil {
		return nil, charErr(err)
	}
	// 如果键不存在，value 为 nil
	if value == nil {
		return nil, nil
	}

	goValue := C.GoBytes(unsafe.Pointer(value), C.int(valLen))
	C.free(unsafe.Pointer(value))

	return goValue, nil
}
func (db *RocksDb) Delete(key []byte) error {
	if db.db == nil {
		return errHandleIsNil
	}
	cKey, keyLen := toCBytes(key)
	var err *C.char
	C.rocksdb_delete(db.db, db.wo, cKey, keyLen, &err)
	return charErr(err)
}

// PutBatch 批量写入键值对, 函数不会对keys进行查重，所以如果key有重复，会被覆盖
func (db *RocksDb) PutBatch(keys, values [][]byte) error {
	if db.db == nil {
		return errHandleIsNil
	}
	if keys == nil || values == nil {
		return errKeyIsNil
	}
	if len(keys) != len(values) {
		return errors.New("keys and values must correspond one to one")
	}
	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)
	count := len(keys)
	for i := 0; i < count; i++ {
		key := keys[i]
		value := values[i]
		cKey, keyLen := toCBytes(key)
		cValue, valLen := toCBytes(value)
		C.rocksdb_writebatch_put(wb, cKey, keyLen, cValue, valLen)
	}

	var err *C.char
	C.rocksdb_write(db.db, db.wo, wb, &err)
	return charErr(err)
}
func (db *RocksDb) DeleteBatch(keys [][]byte) error {
	if db.db == nil {
		return errHandleIsNil
	}
	if keys == nil {
		return errKeyIsNil
	}
	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)
	for _, key := range keys {
		cKey, keyLen := toCBytes(key)
		C.rocksdb_writebatch_delete(wb, cKey, keyLen)
	}
	var err *C.char
	C.rocksdb_write(db.db, db.wo, wb, &err)
	return charErr(err)
}

// GetMulti 批量获取多个键的值，相对于多次读取更优化, 每个 key 都不能是 nil 否则会报错.
// 如果某个 key 不存在对应的项，则回调函数里不会包含它，也就是只返回存在的项
func (db *RocksDb) GetMulti(keys [][]byte, cb func(key, val []byte)) error {
	if db.db == nil {
		return errHandleIsNil
	}
	if keys == nil {
		return errKeyIsNil
	}
	if cb == nil {
		return errProcIsNil
	}

	// 准备 C 数组
	cKeys := make([]*C.char, len(keys))
	cKeyLens := make([]C.size_t, len(keys))
	for i, key := range keys {
		cKeys[i], cKeyLens[i] = toCBytes(key)
	}

	values := make([]*C.char, len(keys))
	valueLens := make([]C.size_t, len(keys))
	errs := make([]*C.char, len(keys))

	C.rocksdb_multi_get(db.db, db.ro, C.size_t(len(keys)), &cKeys[0], &cKeyLens[0], &values[0], &valueLens[0], &errs[0])

	var err error
	for i := range keys {
		if errs[i] != nil {
			err = charErr(errs[i])
		}
		if values[i] != nil {
			val := C.GoBytes(unsafe.Pointer(values[i]), C.int(valueLens[i]))
			C.free(unsafe.Pointer(values[i]))
			cb(keys[i], val)
		}
	}
	return err
}
func (db *RocksDb) DeletePrefix(prefix []byte) (int, error) {
	if db.db == nil {
		return 0, errHandleIsNil
	}
	iter := C.rocksdb_create_iterator(db.db, db.ro)
	defer C.rocksdb_iter_destroy(iter)
	cPrefix, pfLen := toCBytes(prefix)
	if pfLen == 0 {
		C.rocksdb_iter_seek_to_first(iter)
	} else {
		C.rocksdb_iter_seek(iter, cPrefix, pfLen)
	}

	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)
	// 计数删除的键
	count := 0

	// 遍历并删除匹配前缀的键
	for C.rocksdb_iter_valid(iter) != 0 {
		keyLen := C.size_t(0)
		keyPtr := C.rocksdb_iter_key(iter, &keyLen)

		if pfLen > 0 && (keyLen < pfLen || C.memcmp(unsafe.Pointer(keyPtr), unsafe.Pointer(cPrefix), pfLen) != 0) {
			break
		}
		C.rocksdb_writebatch_delete(wb, keyPtr, keyLen)

		count++
		C.rocksdb_iter_next(iter)
	}

	var err *C.char
	C.rocksdb_write(db.db, db.wo, wb, &err)
	return count, charErr(err)
}

// ListPrefix 列出指定前缀的项，返回 false 终止，如何要列出全部项，传入一个长度为 0 的 prefix，但是不能是 nil，防止误操作
func (db *RocksDb) ListPrefix(prefix []byte, cb func(key, val []byte) bool) {
	if db.db == nil {
		return
	}
	if cb == nil {
		return
	}
	iter := C.rocksdb_create_iterator(db.db, db.ro)
	defer C.rocksdb_iter_destroy(iter)
	cPrefix, pfLen := toCBytes(prefix)
	if pfLen == 0 {
		C.rocksdb_iter_seek_to_first(iter)
	} else {
		C.rocksdb_iter_seek(iter, cPrefix, pfLen)
	}

	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)

	for C.rocksdb_iter_valid(iter) != 0 {
		keyLen := C.size_t(0)
		keyPtr := C.rocksdb_iter_key(iter, &keyLen)

		if pfLen > 0 && (keyLen < pfLen || C.memcmp(unsafe.Pointer(keyPtr), unsafe.Pointer(cPrefix), pfLen) != 0) {
			break
		}
		key := C.GoBytes(unsafe.Pointer(keyPtr), C.int(keyLen))
		var valLen C.size_t
		valPtr := C.rocksdb_iter_value(iter, &valLen)
		value := C.GoBytes(unsafe.Pointer(valPtr), C.int(valLen))
		if !cb(key, value) {
			break
		}

		C.rocksdb_iter_next(iter)
	}
}

// ListRange 列出指定范围的键值对, key == start, 在范围内，key == end 不在范围内
// start 和 end 长度都为 0 时，不会返回全部条目，遍历全部键使用 ListPrefix(nil,cb)
func (db *RocksDb) ListRange(start, end []byte, cb func(key, val []byte) bool) {
	if db.db == nil {
		return
	}
	if cb == nil {
		return
	}
	iter := C.rocksdb_create_iterator(db.db, db.ro)
	defer C.rocksdb_iter_destroy(iter)
	cPrefix, pfLen := toCBytes(start)
	if pfLen == 0 {
		C.rocksdb_iter_seek_to_first(iter)
	} else {
		C.rocksdb_iter_seek(iter, cPrefix, pfLen)
	}

	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)

	for C.rocksdb_iter_valid(iter) != 0 {
		keyLen := C.size_t(0)
		keyPtr := C.rocksdb_iter_key(iter, &keyLen)

		key := C.GoBytes(unsafe.Pointer(keyPtr), C.int(keyLen))
		if bytes.Compare(key, end) >= 0 {
			break
		}
		var valLen C.size_t
		valPtr := C.rocksdb_iter_value(iter, &valLen)
		value := C.GoBytes(unsafe.Pointer(valPtr), C.int(valLen))
		if !cb(key, value) {
			break
		}

		C.rocksdb_iter_next(iter)
	}
}
