package main

import (
	"fmt"
	"github.com/jsuserapp/ju"
	"github.com/jsuserapp/rocksdb"
	"os"
	"sync"
	"time"
)

//如果一个数据库使用 ColumnFamily 模式打开，且被添加多于 1 个 column family，则不能再用 RocksDb 打开
//rocksdb 必须打开全部 column family，但是 RocksDb 模式只能打开 “default” 这种缺省模式的数据库

const (
	testDbPath = "./tmp/testdb"
)

func main() {
	//ver := rocksdb.GetVersion()
	//ju.LogGreen(ver)
	//setErrLang()
	//testRocks()
	rocksdb.TestBackup()
}
func setErrLang() {
	lang := os.Getenv("LANG")
	if lang == "" {
		lang = os.Getenv("LC_ALL")
	}
	if lang == "" {
		//Windows环境，需要中文转码
		rocksdb.SetErrLang(func(err []byte) string {
			str, e := ju.GbkToUtf8(err)
			if ju.CheckFailure(e) {
				ju.LogRed(e.Error())
			}
			return str
		})
	}
}

// 打开或创建数据库，如果数据库已经存在且包含多于 1 个 column family，则会报错，
// rocksdb要求打开时指定全部 column family。
func createDb() {
	options := rocksdb.GetDefaultOptions()
	db, err := rocksdb.Open(testDbPath, options)
	if ju.CheckFailure(err) {
		return
	}
	db.Close()
}
func testRocks() {
	var span ju.TimeSpan
	span.Start()
	//数据库会有一个默认的 column family 名称是 “default”，这里添加另一个 “tab1”
	db := openCf([]string{"tab1"})
	if db == nil {
		return
	}
	defer db.Close()
	//列出所有的 cf
	cfs := db.ListColumnFamily()
	ju.LogGreen("column list:", cfs)
	span.LogGreen("list")

	cf := db.GetColumnFamily("tab1")
	if cf == nil {
		return
	}
	//start := []byte("")
	end := []byte("002")
	err := cf.DeleteRange(nil, end)
	if ju.CheckFailure(err) {
		return
	}
	//读写测试
	//span.Start()
	//asynWriteRead(cf)
	//span.LogGreen("write read")
	//列出所有key
	span.Start()
	listItems(cf)
	span.LogGreen("list")
	//批量添加
	//span.Start()
	//putItems(cf, 100, 20)
	//span.LogGreen("put batch")
	//一次取出多个key对应的值
	//span.Start()
	//multiGet(cf, 100, 20)
	//span.LogGreen("multiget")
	//删除项
	//span.Start()
	//deleteItems(cf, 100, 20)
	//span.LogGreen("delete")
}
func putItems(cf *rocksdb.ColumnFamily, start, count int) {
	end := start + count
	var keys, values [][]byte
	for i := start; i < end; i++ {
		ks := fmt.Sprintf("%03d", i)
		k := []byte(ks)
		keys = append(keys, k)

		vs := fmt.Sprintf("%03d_%d", i, time.Now().UnixMicro())
		v := []byte(vs)
		values = append(values, v)
	}
	err := cf.PutBatch(keys, values)
	if err != nil {
		ju.LogRed(err)
	}
}
func deleteItems(cf *rocksdb.ColumnFamily, start, count int) {
	end := start + count
	var keys [][]byte
	for i := start; i < end; i++ {
		ks := fmt.Sprintf("%03d", i)
		k := []byte(ks)
		keys = append(keys, k)
	}
	err := cf.DeleteBatch(keys)
	if err != nil {
		ju.LogRed(err)
	}
}
func multiGet(cf *rocksdb.ColumnFamily, start, count int) {
	end := start + count
	var keys [][]byte
	for i := start; i < end; i++ {
		ks := fmt.Sprintf("%03d", i)
		k := []byte(ks)
		keys = append(keys, k)
	}
	err := cf.GetMulti(keys, func(key, val []byte) {
		ju.LogGreen("Multi Get: ", string(key), "=", string(val))
	})
	if err != nil {
		ju.LogRed(err)
	}
}
func listItems(cf *rocksdb.ColumnFamily) {
	pf := make([]byte, 0)
	cf.ListPrefix(pf, func(key, val []byte) bool {
		ju.LogGreen("List Item:", string(key), "=", string(val))
		//可以在这里删除项目
		//err := cf.Delete(key)
		//if err != nil {
		//	ju.LogRed(err)
		//}
		return true
	})
}
func asynWriteRead(cf *rocksdb.ColumnFamily) {
	var wg sync.WaitGroup
	n := 9
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			key := fmt.Sprintf("%03d", id)
			val := fmt.Sprintf("%03d_%d", id, time.Now().UnixNano())
			err := cf.Put([]byte(key), []byte(val))
			if err != nil {
				ju.LogRed(err.Error())
			}
			wg.Done()
		}(i)
	}
	//多读2个键，会返读取成功，但是值为 nil
	n += 2
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			key := fmt.Sprintf("%03d", id)
			val, err := cf.Get([]byte(key))
			if err != nil {
				ju.LogRed(err.Error())
			} else {
				//因为是异步读写，读和写是同时进行的，所以可能出现读出来为空的情况
				if val == nil {
					ju.LogRed("does not exist:", key)
				} else {
					ju.LogGreen(key, "=", string(val))
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
func openCf(cfs []string) *rocksdb.Db {
	options := rocksdb.GetDefaultOptions()
	db, err := rocksdb.Open(testDbPath, options)
	if err != nil {
		ju.LogRed(err.Error())
		return nil
	}
	db.AddColumnFamily(cfs, options)
	return db
}
func deleteCf() {
	options := rocksdb.GetDefaultOptions()
	dbcf, err := rocksdb.Open(testDbPath, options)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer dbcf.Close()

	_, err = dbcf.DeleteColumnFamily("tab2")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	cfList := dbcf.ListColumnFamily()
	fmt.Println(cfList)

	cf := dbcf.GetColumnFamily("tab1")
	key := []byte("hello")
	//err = cf.Put(key, []byte("world"))
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	val, err := cf.Get(key)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("value", string(val))
}
func addCf() {
	options := rocksdb.GetDefaultOptions()
	cfNames := []string{"tab3"}
	dbcf, err := rocksdb.Open(testDbPath, options)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer dbcf.Close()

	dbcf.AddColumnFamily(cfNames, options)

	cfList := dbcf.ListColumnFamily()
	fmt.Println(cfList)

	cf := dbcf.GetColumnFamily("tab1")
	key := []byte("hello")
	//err = cf.Put(key, []byte("world"))
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	val, err := cf.Get(key)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("value", string(val))
}
