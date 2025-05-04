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
	"errors"
	"github.com/jsuserapp/ju"
	"sync"
	"unsafe"
)

type dbType struct {
	db *C.rocksdb_t
	wo *C.rocksdb_writeoptions_t
	ro *C.rocksdb_readoptions_t
}

var errKeyIsNil = errors.New("key Can't be nil")
var errProcIsNil = errors.New("call back function cannot be nil")
var errHandleIsNil = errors.New("handle is nil, it has closed or not inited")

// SetErrLang rocksdb 返回的错误字符串编码是当前运行环境的语言编码相关的，必然运行环境是中文GBK，
// 则需要相应的转码才能正确显示内容。鉴于语言编码众多，用户自行设置转码操作。如果不设置这个函数，默认
// 就按 utf-8 编码来对待，大多数因为字符可以正确显示，但是如果当前平台编码不是 utf-8，则可能乱码。
func SetErrLang(errLangString func(err []byte) string) {
	_errLangString = errLangString
}

type Db struct {
	mut    sync.Mutex
	cfList *ju.OrderMap[string, *ColumnFamily]
}

func Open(path string, opts *Options) (*Db, error) {
	//尝试创建数据库，因为后面的操作需要数据库必须存在。
	//如果数据库已经存在，这个操作可能会打开失败，忽略它。
	if opts == nil {
		opts = GetDefaultOptions()
		defer opts.Close()
	}
	tryCreateDb(path, opts)

	cfs := ju.NewOrderMap[string, *ColumnFamily]()
	dbcf := &Db{cfList: cfs}
	//如果用户没有传入 options 使用缺省设置
	//数据库路径，这个参数多次使用
	dbPath := C.CString(path)
	defer func() {
		C.free(unsafe.Pointer(dbPath))
	}()
	//获取已经存在的 column family
	existNames, e := getExistCfNames(opts.handle, dbPath)
	if e != nil {
		return nil, e
	}
	for existName := range existNames {
		dbcf.cfList.Set(existName, nil)
	}
	e = dbcf.openExistCf(opts.handle, dbPath, existNames)
	if e != nil {
		return nil, e
	}
	dbcf.initDb(opts)

	return dbcf, nil
}

func (rdb *Db) GetColumnFamily(name string) *ColumnFamily {
	rdb.mut.Lock()
	defer rdb.mut.Unlock()
	cf, _ := rdb.cfList.Get(name)
	return cf
}
func (rdb *Db) GetDefault() *ColumnFamily {
	rdb.mut.Lock()
	defer rdb.mut.Unlock()
	cf, _ := rdb.cfList.Get("default")
	return cf
}
func (rdb *Db) Close() {
	rdb.mut.Lock()
	defer rdb.mut.Unlock()
	for _, cf := range rdb.cfList.Values() {
		cf.Close()
	}
	cf, _ := rdb.cfList.Get("default")
	if cf != nil && cf.rocks != nil {
		rocks := cf.rocks
		if rocks.wo != nil {
			C.rocksdb_writeoptions_destroy(rocks.wo)
			rocks.wo = nil
		}
		if rocks.ro != nil {
			C.rocksdb_readoptions_destroy(rocks.ro)
			rocks.ro = nil
		}
		if rocks.db != nil {
			C.rocksdb_close(rocks.db)
			rocks.db = nil
		}
	}
}
func (rdb *Db) DeleteColumnFamily(name string) (bool, error) {
	rdb.mut.Lock()
	defer rdb.mut.Unlock()
	cf, _ := rdb.cfList.Get(name)
	if cf == nil {
		return false, nil
	}
	var err *C.char
	C.rocksdb_drop_column_family(cf.rocks.db, cf.handle, &err)
	if err != nil {
		return false, charErr(err)
	}
	rdb.cfList.Delete(name)
	cf.Close()
	return true, nil
}
func (rdb *Db) ListColumnFamily() []string {
	rdb.mut.Lock()
	defer rdb.mut.Unlock()
	return rdb.cfList.Keys()
}

// AddColumnFamily 添加 column family，这个函数会先检测要添加的是否已经存在，如果已经存在，
// 直接返回成功，不做任何更改
func (rdb *Db) AddColumnFamily(addNames []string, opts *Options) bool {
	//检测名称的有效性和去重
	addNames = uniqNames(addNames)
	if len(addNames) > 0 {
		var createNames []string
		for _, name := range addNames {
			_, ok := rdb.cfList.Get(name)
			if !ok {
				createNames = append(createNames, name)
			}
		}

		if opts == nil {
			opts = GetDefaultOptions()
			defer opts.Close()
		}
		//生成不存在的 column family
		e := rdb.createCf(opts.handle, createNames)
		if ju.CheckFailure(e) {
			return false
		}
	}
	return true
}
func (rdb *Db) openExistCf(opts *C.rocksdb_options_t, dbPath *C.char, existNames map[string]bool) error {
	count := len(existNames)
	names := make([]string, 0, count)
	for cfName := range existNames {
		names = append(names, cfName)
	}
	//打开 column family
	cfNamesC := make([]*C.char, count)
	for i, cfNameC := range names {
		cfNamesC[i] = C.CString(cfNameC)
	}
	defer func() {
		for _, cfNameC := range cfNamesC {
			C.free(unsafe.Pointer(cfNameC))
		}
	}()
	cfHandles := make([]*C.rocksdb_column_family_handle_t, count)

	cfOpts := make([]*C.rocksdb_options_t, count)
	for i := range cfOpts {
		cfOpts[i] = C.rocksdb_options_create_copy(opts)
	}

	var err *C.char
	//rocksdb_t* rocksdb_open_column_families(
	//    const rocksdb_options_t* options, const char* name, int num_column_families,
	//    const char* const* column_family_names,
	//    const rocksdb_options_t* const* column_family_options,
	//    rocksdb_column_family_handle_t** column_family_handles, char** errptr);
	handle := C.rocksdb_open_column_families(opts, dbPath, C.int(count), &cfNamesC[0], &cfOpts[0], &cfHandles[0], &err)
	for i := range cfOpts {
		C.rocksdb_options_destroy(cfOpts[i])
	}
	if err != nil {
		return charErr(err)
	}

	rocks := &dbType{
		db: handle,
	}
	for i, name := range names {
		rdb.cfList.Set(name, &ColumnFamily{
			rocks:  rocks,
			handle: cfHandles[i],
		})
	}
	return nil
}

// createCf 数据库必须已经打开，default 必然存在
func (rdb *Db) createCf(opts *C.rocksdb_options_t, createNames []string) error {
	createCount := len(createNames)
	if createCount == 0 {
		return nil
	}
	createNamesC := make([]*C.char, createCount)
	for i, createName := range createNames {
		createNamesC[i] = C.CString(createName)
	}
	//一旦赋值后就需要清理
	defer func() {
		for _, createNameC := range createNamesC {
			C.free(unsafe.Pointer(createNameC))
		}
	}()

	//生成需要的 column family
	var err *C.char
	var lencfs C.size_t

	rocks := rdb.GetDefault().rocks
	handleList := C.rocksdb_create_column_families(rocks.db, opts, C.int(createCount), &createNamesC[0], &lencfs, &err)
	if err != nil {
		return charErr(err)
	}
	handleArray := (*[1 << 30]*C.rocksdb_column_family_handle_t)(unsafe.Pointer(handleList))[:lencfs:lencfs]
	handles := make([]*C.rocksdb_column_family_handle_t, lencfs)
	copy(handles, handleArray)
	C.free(unsafe.Pointer(handleList))
	for i := 0; i < int(lencfs); i++ {
		rdb.cfList.Set(createNames[i], &ColumnFamily{
			rocks:  rocks,
			handle: handles[i],
		})
	}
	return nil
}
func (rdb *Db) initDb(options *Options) {
	rocks := rdb.GetDefault().rocks
	//所有cf共享同一个rocks
	rocks.wo = C.rocksdb_writeoptions_create()
	if options != nil && options.DisableWAL {
		C.rocksdb_writeoptions_disable_WAL(rocks.wo, C.int(boolToCint(options.DisableWAL)))
	}
	rocks.ro = C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_prefix_same_as_start(rocks.ro, 1)
}
