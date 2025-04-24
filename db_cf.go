package jurocksdb

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
	"fmt"
	"github.com/jsuserapp/ju"
	"sync"
	"unsafe"
)

type ColumnFamily struct {
	db     *RocksDb
	handle *C.rocksdb_column_family_handle_t
}
type RocksCf struct {
	mut    sync.Mutex
	cfList *ju.OrderMap[string, *ColumnFamily]
}

func (dbcf *RocksCf) GetCf(name string) *ColumnFamily {
	dbcf.mut.Lock()
	defer dbcf.mut.Unlock()
	cf, _ := dbcf.cfList.Get(name)
	return cf
}
func (dbcf *RocksCf) Close() {
	dbcf.mut.Lock()
	defer dbcf.mut.Unlock()
	var db *RocksDb
	for _, cf := range dbcf.cfList.Values() {
		db = cf.db
		cf.Close()
	}
	if db != nil {
		db.Close()
	}
}
func (dbcf *RocksCf) DeleteCf(name string) (bool, error) {
	dbcf.mut.Lock()
	defer dbcf.mut.Unlock()
	cf, _ := dbcf.cfList.Get(name)
	if cf == nil {
		return false, nil
	}
	var err *C.char
	C.rocksdb_drop_column_family(cf.db.db, cf.handle, &err)
	if err != nil {
		return false, charErr(err)
	}
	dbcf.cfList.Delete(name)
	cf.Close()
	return true, nil
}
func (dbcf *RocksCf) ListCf() []string {
	dbcf.mut.Lock()
	defer dbcf.mut.Unlock()
	return dbcf.cfList.Keys()
}
func uniqNames(names []string) []string {
	nm := map[string]bool{}
	for _, name := range names {
		if name == "" {
			continue
		}
		nm[name] = true
	}
	names = make([]string, 0, len(nm))
	for name := range nm {
		names = append(names, name)
	}
	return names
}
func getOptions(options *Options) *C.rocksdb_options_t {
	if options == nil {
		options = GetDefaultOptions()
	}
	opts := C.rocksdb_options_create()
	//应用设置
	applyOptions(opts, options)
	return opts
}
func getExistCfNames(opts *C.rocksdb_options_t, dbPath *C.char) (map[string]bool, error) {
	var lencf C.size_t
	var err *C.char
	existNamesC := C.rocksdb_list_column_families(opts, dbPath, &lencf, &err)
	if err != nil {
		return nil, charErr(err)
	}
	existNames := map[string]bool{}
	namesCArr := (*[1 << 30]*C.char)(unsafe.Pointer(existNamesC))[:lencf:lencf]
	for i := 0; i < int(lencf); i++ {
		existName := C.GoString(namesCArr[i])
		existNames[existName] = true
	}
	C.rocksdb_list_column_families_destroy(existNamesC, lencf)
	return existNames, nil
}
func openExistCf(opts *C.rocksdb_options_t, dbPath *C.char, existNames map[string]bool, cfList *ju.OrderMap[string, *ColumnFamily]) (*RocksDb, error) {
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
		cfOpts[i] = C.rocksdb_options_create()
	}

	var err *C.char
	_db := C.rocksdb_open_column_families(opts, dbPath, C.int(count), &cfNamesC[0], &cfOpts[0], &cfHandles[0], &err)
	for i := range cfOpts {
		C.rocksdb_options_destroy(cfOpts[i])
	}
	if err != nil {
		fmt.Println(_db, cfHandles[0])
		return nil, charErr(err)
	}
	db := RocksDb{db: _db}
	for i, name := range names {
		cfList.Set(name, &ColumnFamily{
			db:     &db,
			handle: cfHandles[i],
		})
	}
	return &db, nil
}
func createCf(db *RocksDb, opts *C.rocksdb_options_t, createNames []string, cfList *ju.OrderMap[string, *ColumnFamily]) error {
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
	handleList := C.rocksdb_create_column_families(db.db, opts, C.int(createCount), &createNamesC[0], &lencfs, &err)
	if err != nil {
		return charErr(err)
	}
	handleArray := (*[1 << 30]*C.rocksdb_column_family_handle_t)(unsafe.Pointer(handleList))[:lencfs:lencfs]
	handles := make([]*C.rocksdb_column_family_handle_t, lencfs)
	copy(handles, handleArray)
	C.free(unsafe.Pointer(handleList))
	for i := 0; i < int(lencfs); i++ {
		cfList.Set(createNames[i], &ColumnFamily{
			db:     db,
			handle: handles[i],
		})
	}
	return nil
}
func initDb(db *RocksDb, options *Options) {
	db.wo = C.rocksdb_writeoptions_create()
	if options.DisableWAL {
		C.rocksdb_writeoptions_disable_WAL(db.wo, C.int(boolToInt(options.DisableWAL)))
	}
	db.ro = C.rocksdb_readoptions_create()
	C.rocksdb_readoptions_set_prefix_same_as_start(db.ro, 1)
}
func OpenCf(path string, options *Options, addNames []string) (*RocksCf, error) {
	cfs := ju.NewOrderMap[string, *ColumnFamily]()
	dbcf := &RocksCf{cfList: cfs}
	//如果用户没有传入 options 使用缺省设置
	opts := getOptions(options)
	//数据库路径，这个参数多次使用
	dbPath := C.CString(path)
	defer func() {
		C.free(unsafe.Pointer(dbPath))
		C.rocksdb_options_destroy(opts)
	}()
	//获取已经存在的 column family
	existNames, e := getExistCfNames(opts, dbPath)
	if e != nil {
		return nil, e
	}
	for existName := range existNames {
		dbcf.cfList.Set(existName, nil)
	}
	db, e := openExistCf(opts, dbPath, existNames, dbcf.cfList)
	if e != nil {
		return nil, e
	}
	initDb(db, options)

	//检测名称的有效性和去重
	addNames = uniqNames(addNames)
	if len(addNames) > 0 {
		var createNames []string
		for _, name := range addNames {
			_, ok := dbcf.cfList.Get(name)
			if !ok {
				createNames = append(createNames, name)
			}
		}
		//生成不存在的 column family
		e = createCf(db, opts, createNames, dbcf.cfList)
		if e != nil {
			return nil, e
		}
	}
	return dbcf, nil
}
func (cf *ColumnFamily) Close() {
	if cf.handle != nil {
		C.rocksdb_column_family_handle_destroy(cf.handle)
		cf.handle = nil
	}
}
func (cf *ColumnFamily) Put(key, value []byte) error {
	var err *C.char
	cKey, keyLen := toCBytes(key)
	cValue, valLen := toCBytes(value)
	C.rocksdb_put_cf(cf.db.db, cf.db.wo, cf.handle, cKey, keyLen, cValue, valLen, &err)
	return charErr(err)
}
func (cf *ColumnFamily) Get(key []byte) ([]byte, error) {
	cKey, keyLen := toCBytes(key)
	var err *C.char
	var valLen C.size_t
	value := C.rocksdb_get_cf(cf.db.db, cf.db.ro, cf.handle, cKey, keyLen, &valLen, &err)
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
func (cf *ColumnFamily) Delete(key []byte) error {
	cKey, keyLen := toCBytes(key)
	var err *C.char
	C.rocksdb_delete_cf(cf.db.db, cf.db.wo, cf.handle, cKey, keyLen, &err)
	return charErr(err)
}

// PutBatch 批量写入键值对, 函数不会对keys进行查重，所以如果key有重复，会被覆盖
func (cf *ColumnFamily) PutBatch(keys, values [][]byte) error {
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
		C.rocksdb_writebatch_put_cf(wb, cf.handle, cKey, keyLen, cValue, valLen)
	}

	var err *C.char
	C.rocksdb_write(cf.db.db, cf.db.wo, wb, &err)
	return charErr(err)
}
func (cf *ColumnFamily) DeleteBatch(keys [][]byte) error {
	if keys == nil {
		return errKeyIsNil
	}
	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)
	for _, key := range keys {
		cKey, keyLen := toCBytes(key)
		C.rocksdb_writebatch_delete_cf(wb, cf.handle, cKey, keyLen)
	}
	var err *C.char
	C.rocksdb_write(cf.db.db, cf.db.wo, wb, &err)
	return charErr(err)
}

// GetMulti 批量获取多个键的值，相对于多次读取更优化, 每个 key 都不能是 nil 否则会报错.
// 如果某个 key 不存在对应的项，则回调函数里不会包含它，也就是只返回存在的项
func (cf *ColumnFamily) GetMulti(keys [][]byte, cb func(key, val []byte)) error {
	if keys == nil {
		return errKeyIsNil
	}
	if cb == nil {
		return errProcIsNil
	}

	numKey := len(keys)
	if numKey == 0 {
		return nil
	}

	// 使用 C 内存分配数组
	cKeys := (**C.char)(C.malloc(C.size_t(numKey) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	cKeyLens := (*C.size_t)(C.malloc(C.size_t(numKey) * C.size_t(unsafe.Sizeof(C.size_t(0)))))
	cCFs := (**C.rocksdb_column_family_handle_t)(C.malloc(C.size_t(numKey) * C.size_t(unsafe.Sizeof((*C.rocksdb_column_family_handle_t)(nil)))))
	values := (**C.char)(C.malloc(C.size_t(numKey) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	valueLens := (*C.size_t)(C.malloc(C.size_t(numKey) * C.size_t(unsafe.Sizeof(C.size_t(0)))))
	errs := (*C.char)(C.malloc(C.size_t(numKey) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))

	// 确保释放 C 内存
	defer func() {
		C.free(unsafe.Pointer(cKeys))
		C.free(unsafe.Pointer(cKeyLens))
		C.free(unsafe.Pointer(cCFs))
		C.free(unsafe.Pointer(values))
		C.free(unsafe.Pointer(valueLens))
		C.free(unsafe.Pointer(errs))
	}()
	// 填充数组
	keyPtrs := (*[1 << 30]*C.char)(unsafe.Pointer(cKeys))[:numKey:numKey]
	keyLenPtrs := (*[1 << 30]C.size_t)(unsafe.Pointer(cKeyLens))[:numKey:numKey]
	cfPtrs := (*[1 << 30]*C.rocksdb_column_family_handle_t)(unsafe.Pointer(cCFs))[:numKey:numKey]
	valuePtrs := (*[1 << 30]*C.char)(unsafe.Pointer(values))[:numKey:numKey]
	valueLenPtrs := (*[1 << 30]C.size_t)(unsafe.Pointer(valueLens))[:numKey:numKey]
	errPtrs := (*[1 << 30]*C.char)(unsafe.Pointer(errs))[:numKey:numKey]

	for i, key := range keys {
		keyPtrs[i], keyLenPtrs[i] = toCBytes(key)
		cfPtrs[i] = cf.handle // 同一列族
		valuePtrs[i] = nil    // 初始化
		valueLenPtrs[i] = 0
		errPtrs[i] = nil
	}

	C.rocksdb_multi_get_cf(cf.db.db, cf.db.ro, cCFs, C.size_t(numKey), cKeys, cKeyLens, values, valueLens, &errs)

	var err error
	for i := range keys {
		if errPtrs[i] != nil {
			err = charErr(errPtrs[i])
		}
		if valuePtrs[i] != nil {
			val := C.GoBytes(unsafe.Pointer(valuePtrs[i]), C.int(valueLenPtrs[i]))
			C.rocksdb_free(unsafe.Pointer(valuePtrs[i]))
			cb(keys[i], val)
		}
	}

	return err
}
func (cf *ColumnFamily) DeletePrefix(prefix []byte) (int, error) {
	iter := C.rocksdb_create_iterator_cf(cf.db.db, cf.db.ro, cf.handle)
	defer C.rocksdb_iter_destroy(iter)
	cPrefix, pfLen := toCBytes(prefix)
	if pfLen == 0 {
		C.rocksdb_iter_seek_to_first(iter)
	} else {
		C.rocksdb_iter_seek(iter, cPrefix, pfLen)
	}

	wb := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(wb)
	count := 0

	for C.rocksdb_iter_valid(iter) != 0 {
		keyLen := C.size_t(0)
		keyPtr := C.rocksdb_iter_key(iter, &keyLen)

		if pfLen > 0 && (keyLen < pfLen || C.memcmp(unsafe.Pointer(keyPtr), unsafe.Pointer(cPrefix), pfLen) != 0) {
			break
		}
		C.rocksdb_writebatch_delete_cf(wb, cf.handle, keyPtr, keyLen)

		count++
		C.rocksdb_iter_next(iter)
	}

	var err *C.char
	C.rocksdb_write(cf.db.db, cf.db.wo, wb, &err)
	return count, charErr(err)
}

// ListPrefix 列出指定前缀的项，返回 false 终止，如何要列出全部项，传入一个长度为 0 的 prefix，但是不能是 nil，防止误操作
func (cf *ColumnFamily) ListPrefix(prefix []byte, cb func(key, val []byte) bool) {
	if cb == nil {
		return
	}
	iter := C.rocksdb_create_iterator_cf(cf.db.db, cf.db.ro, cf.handle)
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
func (cf *ColumnFamily) ListRange(start, end []byte, cb func(key, val []byte) bool) {
	if cb == nil {
		return
	}
	iter := C.rocksdb_create_iterator_cf(cf.db.db, cf.db.ro, cf.handle)
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
