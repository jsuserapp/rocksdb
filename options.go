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

// Options 定义 RocksDB 的数据库打开选项
type Options struct {
	handle *C.rocksdb_options_t
	// CreateIfMissing 如果数据库不存在，是否创建新数据库
	// 默认值: true
	// 设为 false 时，若数据库不存在，打开会失败
	CreateIfMissing bool

	//CreateColumnFamiliesIfMissing 打开数据库时自动生成不存在的 column families
	//当前默认值 true，rocksdb 内部默认值是 false
	CreateColumnFamiliesIfMissing bool

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
	// 设为 true 时，禁运 WAL 数据库降不会再记录写日志，但是数据库崩溃时，可能造成数据丢失。
	DisableWAL bool

	// CompressionType 数据压缩类型，影响存储空间和读写性能
	// 默认值: "snappy" (支持: "none", "snappy", "zlib", "bzip2", "lz4", "zstd")
	// "none" 表示无压缩，"snappy" 平衡速度和压缩率
	CompressionType string

	// TargetFileSizeBase 每个 SST 文件的目标大小（字节）
	// 默认值: 0
	// 影响压缩和读取性能，小值增加文件数，大值减少文件数
	TargetFileSizeBase uint64

	// MaxBackgroundJobs 后台任务（如压缩、刷盘）的最大线程数
	// 默认值: 0
	// 增大可提升后台处理速度，但消耗更多 CPU
	MaxBackgroundJobs int

	// AllowConcurrentMemtableWrite 是否允许多线程并发写入 MemTable
	// 默认值: false (RocksDB 6.7+ 支持)
	// 设为 true 可提升多线程写性能
	AllowConcurrentMemtableWrite bool

	//KeepLogFileNum 控制 RocksDB 操作日志（LOG 和 LOG.old.* 文件）的保留数量
	KeepLogFileNum int

	//RecycleLogFileNum 控制 RocksDB 中 WAL 文件(.log)的回收数量，用于减少文件系统的创建和删除开销。
	//默认值：0，表示不回收 WAL 文件，过期后直接删除。
	RecycleLogFileNum int
}

// GetDefaultOptions 返回默认的 RocksDB 选项
func GetDefaultOptions() *Options {
	opt := &Options{
		CreateIfMissing:               true, // 如果数据库不存在，自动创建
		CreateColumnFamiliesIfMissing: true,
		IncreaseParallelism:           0,
		ErrorIfExists:                 false, // 允许覆盖现有数据库
		WriteBufferSize:               0,
		MaxOpenFiles:                  0,
		DisableWAL:                    false,    // 默认启用 WAL
		CompressionType:               "snappy", // 使用 Snappy 压缩
		TargetFileSizeBase:            0,
		MaxBackgroundJobs:             0,     // 后台任务线程
		AllowConcurrentMemtableWrite:  false, // 默认禁用并发 MemTable 写
	}
	opt.handle = C.rocksdb_options_create()
	C.rocksdb_options_set_create_missing_column_families(opt.handle, 1)
	return opt
}

// Set 将 Go 的 Options 应用到 RocksDB 的 C 选项
func (opt *Options) Set() {
	C.rocksdb_options_set_create_missing_column_families(opt.handle, boolToUChar(opt.CreateColumnFamiliesIfMissing))
	C.rocksdb_options_set_create_if_missing(opt.handle, boolToUChar(opt.CreateIfMissing))
	C.rocksdb_options_increase_parallelism(opt.handle, C.int(opt.IncreaseParallelism))
	C.rocksdb_options_set_error_if_exists(opt.handle, boolToUChar(opt.ErrorIfExists))
	C.rocksdb_options_set_write_buffer_size(opt.handle, C.size_t(opt.WriteBufferSize))
	C.rocksdb_options_set_max_open_files(opt.handle, C.int(opt.MaxOpenFiles))

	switch opt.CompressionType {
	case "none":
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_no_compression)
	case "snappy":
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_snappy_compression)
	case "zlib":
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_zlib_compression)
	case "bzip2":
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_bz2_compression)
	case "lz4":
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_lz4_compression)
	case "zstd":
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_zstd_compression)
	default:
		C.rocksdb_options_set_compression(opt.handle, C.rocksdb_snappy_compression) // 默认 Snappy
	}

	C.rocksdb_options_set_target_file_size_base(opt.handle, C.uint64_t(opt.TargetFileSizeBase))
	C.rocksdb_options_set_max_background_jobs(opt.handle, C.int(opt.MaxBackgroundJobs))
	C.rocksdb_options_set_allow_concurrent_memtable_write(opt.handle, boolToUChar(opt.AllowConcurrentMemtableWrite))
	C.rocksdb_options_set_keep_log_file_num(opt.handle, C.size_t(opt.KeepLogFileNum))
	C.rocksdb_options_set_recycle_log_file_num(opt.handle, C.size_t(opt.RecycleLogFileNum))
}

// Get Options 绑定了 C 内置资源，用完需要 free 释放
func (opt *Options) Get() {
	opt.CreateColumnFamiliesIfMissing = ucharToBool(C.rocksdb_options_get_create_missing_column_families(opt.handle))
	opt.CreateIfMissing = ucharToBool(C.rocksdb_options_get_create_if_missing(opt.handle))
	opt.ErrorIfExists = ucharToBool(C.rocksdb_options_get_error_if_exists(opt.handle))
	opt.WriteBufferSize = int(C.rocksdb_options_get_write_buffer_size(opt.handle))
	opt.MaxOpenFiles = int(C.rocksdb_options_get_max_open_files(opt.handle))
	opt.TargetFileSizeBase = uint64(C.rocksdb_options_get_target_file_size_base(opt.handle))
	opt.MaxBackgroundJobs = int(C.rocksdb_options_get_max_background_jobs(opt.handle))
	opt.AllowConcurrentMemtableWrite = ucharToBool(C.rocksdb_options_get_allow_concurrent_memtable_write(opt.handle))
	opt.KeepLogFileNum = int(C.rocksdb_options_get_keep_log_file_num(opt.handle))
	opt.RecycleLogFileNum = int(C.rocksdb_options_get_recycle_log_file_num(opt.handle))
}
func (opt *Options) Create() {
	if opt.handle != nil {
		opt.handle = C.rocksdb_options_create()
	}
}

// Close Options 绑定了 C 内置资源，用完需要 free 释放
func (opt *Options) Close() {
	if opt.handle != nil {
		C.rocksdb_options_destroy(opt.handle)
		opt.handle = nil
	}
}
