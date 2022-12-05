package xdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"
)

// DBEngine 是存储引擎，完成段的创建、索引的更新、段的合并和压缩
type DBEngine struct {
	dataDir    string             // 数据文件保存目录
	segFName   string             // 当前处于active的段文件名称
	memIdx     map[string]MemIdxV // 内存hashmap 索引
	segFMu     sync.Mutex         // 当前活跃段文件锁
	memIdxMu   sync.Mutex         // 内存索引锁
	segMergeMu sync.Mutex         // 段合并锁
}

var dbEngine *DBEngine // 数据库引擎对象，全局唯一

// segFLen 获取当前活跃段文件长度
func (engine *DBEngine) segFLen(fName string) int64 {
	fPath := path.Join(engine.dataDir, fName)
	len, err := fSize(fPath)
	if err != nil {
		slogger.Errorf("get active segment file: %s size error: %v", fPath, err)
	}
	return len
}

// appendSeg 将数据写入段文件
func (engine *DBEngine) appendSeg(seg *Segment) error {
	engine.segFMu.Lock()
	defer engine.segFMu.Unlock()
	// 若不存在段文件，或者检测当前段文件大小，若超过限制则重新创建段文件
	if engine.segFName == "" || dbEngine.segFLen(engine.segFName) >= SegSizeLimit {
		segFName, err := engine.newDataF(SegFNameFormat, SegFNamePrefix, time.Now().UnixNano())
		if err != nil {
			slogger.Fatalf("create segment file errror: %v", err)
		}
		engine.segFName = segFName
		slogger.Infof("new segment created, active segment file: %s\n", segFName)
		// 启动段合并流程
		go engine.segMerge()
	}
	segFile, err := os.OpenFile(path.Join(engine.dataDir, engine.segFName), os.O_APPEND|os.O_WRONLY, FileMode)
	defer segFile.Close()
	if err != nil {
		return fmt.Errorf("idxK: %s, value: %s, write segment file error: %v\n", seg.key, seg.value, err)
	}
	dataStr := encodeSeg(seg)
	_, err = segFile.WriteString(dataStr)
	if err != nil {
		return fmt.Errorf("write seg to file: %s error: %v", engine.segFName, err)
	}
	return nil
}

// freezeSegFs 获取已经冻结的所有段文件列表
func (engine *DBEngine) freezeSegFs() []os.DirEntry {
	dataFs := getDataFs(engine.dataDir, SegFNamePrefix, 1)
	return dataFs[1:] // 时间戳最大的段文件为当前活跃段
}

// segMerge 段合并
func (engine *DBEngine) segMerge() {
	engine.segMergeMu.Lock() // 加锁，每次只允许一个goroutine 进行段合并操作
	defer engine.segMergeMu.Unlock()

	//1.获取已冻结的段文件列表
	segFs := engine.freezeSegFs()
	if len(segFs) < MaxSegmentNum {
		return
	}
	// 2. 创建新的段文件和hint file，作为段合并后的数据存储文件
	newSegFName, hintFName, _ := engine.newCompF()
	newSegF, errOpenSegF := os.OpenFile(path.Join(engine.dataDir, newSegFName), os.O_WRONLY|os.O_WRONLY, FileMode)
	hintF, errOpenHintF := os.OpenFile(path.Join(engine.dataDir, hintFName), os.O_WRONLY|os.O_WRONLY, FileMode)
	if errOpenSegF != nil || errOpenHintF != nil {
		slogger.Fatalf("open new seg file or hint file error: %v,%v", errOpenSegF, errOpenHintF)
	}
	defer newSegF.Close()
	defer hintF.Close()
	newSegWriter := bufio.NewWriter(newSegF)
	hintWriter := bufio.NewWriter(hintF)
	var offset int64
	// 3. 遍历已冻结的段文件列表
	for _, segF := range segFs {
		// 如果当前的合并生成的段文件大小超过阈值，创建新的段文件
		if dbEngine.segFLen(newSegFName) > SegSizeLimit {
			newSegFName, hintFName, _ = engine.newCompF()
			newSegF, errOpenSegF = os.OpenFile(path.Join(engine.dataDir, newSegFName), os.O_WRONLY|os.O_WRONLY, FileMode)
			hintF, errOpenHintF = os.OpenFile(path.Join(engine.dataDir, hintFName), os.O_WRONLY|os.O_WRONLY, FileMode)
			if errOpenSegF != nil || errOpenHintF != nil {
				slogger.Fatalf("open new seg file or hint file error: %v,%v", errOpenSegF, errOpenHintF)
			}
			defer newSegF.Close()
			defer hintF.Close()
			newSegWriter = bufio.NewWriter(newSegF)
			hintWriter = bufio.NewWriter(hintF)
		}
		// 3.0 逐行读取原段文件的数据
		f, err := os.OpenFile(path.Join(engine.dataDir, segF.Name()), os.O_RDONLY, FileMode)
		defer f.Close()
		if err != nil {
			slogger.Fatalf("open seg file: %s error: %v", segF.Name(), err)
		}
		reader := bufio.NewReader(f)
		dataStr, err1 := reader.ReadString(DataDelimiterByte)
		for !errors.Is(err1, io.EOF) {
			if err1 != nil {
				slogger.Errorf("read segment file: %s error: %v", segF.Name(), err1)
			}
			seg, err := decodeSeg(dataStr)
			if err != nil {
				slogger.Errorf("decodeHint data: %s error: %v", dataStr, err)
			}
			// 3.1 对于尚未处理且新增/更新的key,进行处理
			if idx, ok := engine.memIdx[seg.key]; ok && idx.fName == segF.Name() && idx.tm == seg.tm {
				// 写入新的segment 文件
				n, err := newSegWriter.WriteString(encodeSeg(seg))
				if err != nil {
					slogger.Errorf("write new segment: %s data: %v error: %v", newSegFName, seg, err)
				}
				offset = offset + int64(n)
				seg.valops = offset - int64(NewLineSize+seg.valsz)
				// 写入hint 文件
				hint := seg2Hint(seg)
				_, err = hintWriter.WriteString(encodeHint(hint))
				if err != nil {
					slogger.Errorf("write seg2Hint: %v error: %v", hint, err)
				}
				// 更新索引
				engine.updMemIdx(segment2MemIndex(seg, newSegFName))
			}
			dataStr, err1 = reader.ReadString(DataDelimiterByte)
		}
		// 3.2 删除已经合并完成的段文件和其hint文件(若存在)
		removeCompF(engine.dataDir, segF.Name(), SegFNamePrefix)
		slogger.Infof("merge segment %s done!\n", segF.Name())
	}
	slogger.Infof("merge segment done! merge segment num: %d to segment: %s\n", len(segFs), newSegFName)
}

// isExistHint 判断当前文件是否存在hint文件
func (engine *DBEngine) isExistCompF(fName, prefix string) bool {
	name, err := compFName(fName, prefix)
	if err != nil {
		slogger.Fatalf("compFName error: %v", err)
	}
	return isExistF(path.Join(engine.dataDir, name))
}

// genIndexStr 生成IndexValue
func (engine *DBEngine) updMemIdx(memIdx *MemIdx) {
	// 校验时间戳
	preIndex, ok := engine.memIdx[memIdx.idxK]
	if ok && preIndex.tm > memIdx.idxV.tm {
		return
	}
	engine.memIdxMu.Lock()
	defer engine.memIdxMu.Unlock()

	if memIdx.idxV.valsz > 0 {
		engine.memIdx[memIdx.idxK] = memIdx.idxV
	} else {
		delete(engine.memIdx, memIdx.idxK)
	}
}

// prsHintF 根据hint文件内容更新索引
func (engine *DBEngine) prsHintF(hintPath string) {
	segFName, err := compFName(path.Base(hintPath), HintFNamePrefix)
	if err != nil {
		slogger.Fatalf("company hintF name error: %v", err)
	}
	hintF, err := os.OpenFile(hintPath, os.O_RDONLY, FileMode)
	defer hintF.Close()
	if err != nil {
		slogger.Fatalf("open hintF: %s path error: %v", hintPath, err)
	}
	reader := bufio.NewReader(hintF)
	dataStr, err := reader.ReadString(DataDelimiterByte)
	for !errors.Is(err, io.EOF) {
		hint, err1 := decodeHint(dataStr)
		if err1 == nil {
			engine.updMemIdx(hint2MemIndex(hint, segFName))
		} else {
			slogger.Infof("decodeHint: %s error: %v", dataStr, err1)
		}
		dataStr, err = reader.ReadString(DataDelimiterByte)
	}
}

// prsSegF 根据段文件生成内存索引
func (engine *DBEngine) prsSegF(segPath string) {
	f, err := os.OpenFile(segPath, os.O_RDONLY, FileMode)
	defer f.Close()
	if err != nil {
		slogger.Fatalf("open f: %s error: %v", segPath, err)
	}
	reader := bufio.NewReader(f)
	var offset int64 // 当前文件读取位置
	dataStr, err := reader.ReadString(DataDelimiterByte)
	for !errors.Is(err, io.EOF) {
		if err != nil {
			slogger.Fatalf("read active segment error: %v", err)
		}
		offset = offset + int64(len(dataStr))
		seg, err1 := decodeSeg(dataStr)
		if err1 != nil {
			slogger.Error(err1)
		} else {
			// 更新索引
			seg.valops = offset - int64(NewLineSize+seg.valsz)
			engine.updMemIdx(segment2MemIndex(seg, path.Base(segPath)))
		}
		dataStr, err = reader.ReadString(DataDelimiterByte)
	}
}

// newDataF 创建新的数据文件
func (engine *DBEngine) newDataF(fNameFormat, fNamePrefix string, fTm int64) (string, error) {
	fName := fmt.Sprintf(fNameFormat, fNamePrefix, fTm)
	fPath := path.Join(engine.dataDir, fName)
	f, err := os.Create(fPath)
	if err != nil {
		return "", fmt.Errorf("create segmentFile error, fPath: %s, error: %v", fPath, err)
	}
	defer f.Close()
	return fName, nil
}

// newCompF 同时创建segment,hint 文件(在进行段合并时使用)
func (engine *DBEngine) newCompF() (string, string, error) {
	fileID := time.Now().Add(SegFIDGap).UnixNano() // 合并生成的段文件的tm 比当前时间小 50 year
	segFName, err := engine.newDataF(SegFNameFormat, SegFNamePrefix, fileID)
	if err != nil {
		slogger.Fatalf("create segment file: %s error: %v", segFName, err)
	}
	hintFName, err := engine.newDataF(HintFNameFormat, HintFNamePrefix, fileID)
	if err != nil {
		slogger.Fatalf("create seg2Hint file: %s error: %v", hintFName, err)
	}
	return segFName, hintFName, nil
}

// genMemIdx 通过hint file 生成 memory memIdx
func (engine *DBEngine) genMemIdx(segFs []os.DirEntry) {
	for _, f := range segFs {
		// 如果有hint file,就使用hint file 生成index
		hintFName, err := compFName(f.Name(), SegFNamePrefix)
		if err != nil {
			slogger.Fatalf("company file error:%v", err)
		}
		hintPath := path.Join(engine.dataDir, hintFName)
		if isExistF(hintPath) {
			engine.prsHintF(hintPath)
			slogger.Infof("parse hint file: %s done.\n", hintPath)
		} else {
			// 否则，就扫描整个段文件生成index
			segPath := path.Join(engine.dataDir, f.Name())
			engine.prsSegF(segPath)
			slogger.Infof("parse segment file: %s done.\n", segPath)
		}
	}
}
