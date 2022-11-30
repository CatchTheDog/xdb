package pkg

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Put 将(idxK,value)键值对保存到数据库中
func Put(key, value string) error {
	if key == "" || value == "" {
		return fmt.Errorf("idxK, value can not be empty, idxK: %s, value: %s", key, value)
	}
	seg := &Segment{
		value: value,
		Hint: Hint{
			key:   key,
			keysz: len(key),
			val: val{
				tm:     time.Now().UnixNano(),
				valsz:  len(value),
				valops: 0,
			},
		},
	}
	// 将数据写入文件
	err := dbEngine.appendSeg(seg)
	if err != nil {
		slogger.Fatalf("write seg to file: %s error: %v", dbEngine.segFName, err)
	}
	// 更新索引
	seg.valops = dbEngine.segFLen(dbEngine.segFName) - int64(NewLineSize+seg.valsz)
	dbEngine.updMemIdx(segment2MemIndex(seg, dbEngine.segFName))
	return nil
}

// Query 从数据库中查找key对应的value并返回
func Query(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%s", "idxK can not be empty")
	}
	indexValue, ok := dbEngine.memIdx[key]
	if ok {
		return seekKey(dbEngine.dataDir, indexValue)
	}
	slogger.Infof("get idxK: %s, no memIdx exist\n", key)
	return "", nil
}

// Remove 从数据库中删除key对应的记录
func Remove(key string) error {
	if key == "" {
		return fmt.Errorf("idxK, value can not be empty, idxK: %s", key)
	}
	// 将数据写入文件
	seg := &Segment{
		Hint: Hint{
			key:   key,
			keysz: len(key),
			val: val{
				tm:     time.Now().UnixNano(),
				valops: 0,
			},
		},
	}
	err := dbEngine.appendSeg(seg)
	if err != nil {
		log.Fatalf("write seg to file: %s error: %v", dbEngine.segFName, err)
	}
	// 更新索引
	dbEngine.updMemIdx(segment2MemIndex(seg, dbEngine.segFName))
	return nil
}

// Open 启动数据库引擎，dataDir指定数据库数据存放目录，若不指定目录则默认：/Users/majunqiang/Documents/mrxdbengine/data/
func Open(dataDir string) {
	dbEngine = &DBEngine{
		dataDir: DataDir,
		memIdx:  make(map[string]MemIdxV),
	}
	// 1. 设置数据目录
	if dataDir != "" {
		dbEngine.dataDir = dataDir
	}
	// 2. 获取数据文件夹下所有的段文件，按照时间戳倒序排列
	segFs := getDataFs(dbEngine.dataDir, SegFNamePrefix, 1)
	if len(segFs) > 0 {
		// 3. 设置当前活跃段文件
		fName := segFs[0].Name()
		dbEngine.segFName = fName
		slogger.Infof("active segment file: %s\n", fName)
		// 4. 从段文件生成内存索引,若段文件有对应的hint file,则使用hint file生成内存索引
		dbEngine.genMemIdx(segFs)
	}
	// 5. 启动完成
	slogger.Infof("dbEngine start success!")
}

// ListKey 返回当前数据库中所有有效的
func ListKey(key string) []string {
	keys := make([]string, 0)
	for k, _ := range dbEngine.memIdx {
		keys = append(keys, k)
	}
	return keys
}

// Sync 将写入数据的数据刷新到磁盘
func Sync() {

}

// Close 关闭当前数据库引擎
func Close() {
	// 执行Sync刷盘
	Sync()
	// 退出
	slogger.Infoln("db engine will exit.")
	os.Exit(0)
}
