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
	"unsafe"
)

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
	var errStr string
	if _errLangString != nil {
		rawBytes := (*[1 << 30]byte)(unsafe.Pointer(err))
		//这个函数通过 null 结尾来判断字符串介绍，安全起见，假设最大错误字串长度不会超过 256
		for i := 0; i < 256; i += 1 {
			if rawBytes[i] == 0 {
				errStr = _errLangString(rawBytes[:i])
				break
			}
		}
	} else {
		errStr = C.GoString(err)
	}
	C.free(unsafe.Pointer(err))
	return errors.New(errStr)
}

var _errLangString func([]byte) string

func boolToUChar(v bool) C.uchar {
	if v {
		return 1
	} else {
		return 0
	}
}
func boolToCint(v bool) C.int {
	if v {
		return 1
	} else {
		return 0
	}
}
func ucharToBool(v C.uchar) bool {
	return v != 0
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
func tryCreateDb(path string, opts *Options) {
	if opts == nil {
		opts = GetDefaultOptions()
		defer opts.Close()
	}
	var err *C.char
	dbPath := C.CString(path)
	handle := C.rocksdb_open(opts.handle, dbPath, &err)
	C.free(unsafe.Pointer(dbPath))
	if err != nil {
		return
	}
	C.rocksdb_close(handle)
}
