package main

import (
	"fmt"
	"github.com/jsuserapp/ju"
	"jurocksdb"
	"sync"
	"time"
)

//如果一个数据库使用 ColumnFamily 模式打开，且被添加多于 1 个 column family，则不能再用 RocksDb 打开
//rocksdb 必须打开全部 column family，但是 RocksDb 模式只能打开 “default” 这种缺省模式的数据库

func main() {
	//testOrderMap()
	testRocks()
}
func testOrderMap() {
	om := ju.NewOrderMap[string, string]()
	om.Set("1", "a")
	om.Set("4", "d")
	om.Set("2", "b")
	om.Set("3", "c")
	om.Set("5", "e")
	om.Set("6", "f")
	keys := om.Keys()
	ju.LogGreen(keys)
	v, _ := om.Get("3")
	ju.LogGreen(v)
	v, _ = om.Get("7")
	ju.LogGreen(v)
}
func testRocks() {
	var span ju.TimeSpan
	span.Start()
	db := openCf()
	if db == nil {
		return
	}
	defer db.Close()
	//列出所有的 cf
	cfs := db.ListCf()
	ju.LogGreen("column list:", cfs)
	span.LogGreen("list")

	cf := db.GetCf("tab1")
	if cf == nil {
		return
	}
	//读写测试
	span.Start()
	//asynWriteRead(cf)
	span.LogGreen("write read")
	//列出所有key
	span.Start()
	//listItems(cf)
	span.LogGreen("list")
	//删除项
	span.Start()
	//deleteItems(cf)
	span.LogGreen("delete")
	//批量添加
	span.Start()
	putItems(cf)
	span.LogGreen("put batch")
	//一次取出多个key对应的值
	span.Start()
	multiGet(cf)
	span.LogGreen("multiget")
}
func putItems(cf *jurocksdb.ColumnFamily) {
	n := 200
	start := 199
	end := start + n
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
func deleteItems(cf *jurocksdb.ColumnFamily) {
	n := 200
	start := 199
	end := start + n
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
func multiGet(cf *jurocksdb.ColumnFamily) {
	n := 200
	start := 199
	end := start + n
	var keys [][]byte
	for i := start; i < end; i++ {
		ks := fmt.Sprintf("%03d", i)
		k := []byte(ks)
		keys = append(keys, k)
	}
	err := cf.GetMulti(keys, func(key, val []byte) {
		ju.LogGreen(string(key), "=", string(val))
	})
	if err != nil {
		ju.LogRed(err)
	}
}
func listItems(cf *jurocksdb.ColumnFamily) {
	cf.ListPrefix([]byte("2"), func(key, val []byte) bool {
		ju.LogGreen(string(key), "=", string(val))
		//err := cf.Delete(key)
		//if err != nil {
		//	ju.LogRed(err)
		//}
		return true
	})
}
func asynWriteRead(cf *jurocksdb.ColumnFamily) {
	var wg sync.WaitGroup
	n := 500
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
	n += 2
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			key := fmt.Sprintf("%03d", id)
			val, err := cf.Get([]byte(key))
			if err != nil {
				ju.LogRed(err.Error())
			} else {
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
func openCf() *jurocksdb.RocksCf {
	options := jurocksdb.GetDefaultOptions()
	db, err := jurocksdb.OpenCf("testdb", options, nil)
	if err != nil {
		ju.LogRed(err.Error())
		return nil
	}
	return db
}
func deleteCf() {
	options := jurocksdb.GetDefaultOptions()
	cfNames := []string{"tab1"}
	dbcf, err := jurocksdb.OpenCf("testdb", options, cfNames)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer dbcf.Close()

	_, err = dbcf.DeleteCf("tab2")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	cfList := dbcf.ListCf()
	fmt.Println(cfList)

	cf := dbcf.GetCf("tab1")
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
	options := jurocksdb.GetDefaultOptions()
	cfNames := []string{"tab3"}
	dbcf, err := jurocksdb.OpenCf("testdb", options, cfNames)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer dbcf.Close()

	cfList := dbcf.ListCf()
	fmt.Println(cfList)

	cf := dbcf.GetCf("tab1")
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
func SomeTest() {
	options := jurocksdb.GetDefaultOptions()
	db, err := jurocksdb.Open("testdb", options)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer db.Close()

	db.ListPrefix([]byte("key_4"), func(key, val []byte) bool {
		fmt.Printf("%s=%s\n", string(key), string(val))
		return true
	})
}
