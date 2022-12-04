package main

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

// isExistF 判断path是否存在
func isExistF(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// fSize 获取文件size
func fSize(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err == nil {
		return stat.Size(), nil
	}
	return 0, err
}

// parseTm 从文件名称中获取文件创建时的时间戳
func parseTm(delimiter, name string) (int64, error) {
	strArr := strings.Split(name, delimiter)
	if len(strArr) == 2 {
		return strconv.ParseInt(strArr[1], 10, 64)
	}
	return 0, fmt.Errorf("name: %s does not contain delimiter: %s", name, delimiter)
}

// listDataFs 检查并扫描数据文件路径，若路径存在，返回其下文件列表；若路径不存在，创建文件路径并返回；
func listDataFs(dataDir string) []os.DirEntry {
	if !isExistF(dataDir) {
		err := os.MkdirAll(dataDir, FileMode)
		if err != nil {
			slogger.Fatalf("create dataDir error, dataDir: %s, error: %v", dataDir, err)
		}
	}
	fs, err := os.ReadDir(dataDir)
	if err != nil {
		slogger.Fatalf("list activeSegment file error, dataDir: %s, error: %v", dataDir, err)
	}
	return fs
}

// classifyFs 对数据文件夹下的文件进行分类，并对每类文件按时间戳倒序排列
func classifyFs(fs []os.DirEntry, prefix string) []os.DirEntry {
	segFs := make([]os.DirEntry, 0)
	for _, f := range fs {
		if strings.HasPrefix(f.Name(), prefix) {
			segFs = append(segFs, f)
		}
	}
	return segFs
}

// getDataFs 返回文件路径path下的所有子文件中文件名称前缀匹配prefix的文件，并按照时间戳指定顺序排序
// order：文件排列顺序，1-倒序 0-顺序
func getDataFs(path, prefix string, order uint) []os.DirEntry {
	return sortDataF(classifyFs(listDataFs(path), prefix), 1)
}

// sortDataF 按照文件时间戳对文件倒序排列
// order： 文件顺序 1- 倒序 0-顺序
func sortDataF(fs []os.DirEntry, order uint) []os.DirEntry {
	sort.Slice(fs, func(i, j int) bool {
		name := fs[i].Name()
		tm, _ := parseTm(Delimiter, name)
		name1 := fs[j].Name()
		tm1, _ := parseTm(Delimiter, name1)
		switch order {
		case ASC:
			return tm < tm1
		case DESC:
			return tm > tm1
		default:
			return false
		}
	})
	return fs
}

// crc 根据数据串生成crc校验值
func crc(dataStr string) uint32 {
	return crc32.ChecksumIEEE([]byte(dataStr))
}

// checkCRC 校验checkSum 与 str生成的校验值是否一致
func checkCRC(str string, checkSum uint32) bool {
	return crc32.ChecksumIEEE([]byte(str)) == checkSum
}

// compFName 获取伙伴文件名称
func compFName(name, prefix string) (string, error) {
	switch prefix {
	case HintFNamePrefix:
		return strings.Replace(name, HintFNamePrefix, SegFNamePrefix, 1), nil
	case SegFNamePrefix:
		return strings.Replace(name, SegFNamePrefix, HintFNamePrefix, 1), nil
	}
	return "", nil
}

// removeCompF 删除segment,hint 文件
func removeCompF(dataDir, name, prefix string) {
	compFName, err := compFName(name, prefix)
	if err != nil {
		slogger.Errorf("name: %s, prefix: %s; get compFName error: %v\n", name, prefix, err)
	}
	fPath := path.Join(dataDir, name)
	if isExistF(fPath) {
		err = os.Remove(fPath)
		if err != nil {
			slogger.Fatalf("delete file: %s error: %v\n", name, err)
		}
	}
	compFPath := path.Join(dataDir, compFName)
	if isExistF(compFPath) {
		err = os.Remove(path.Join(dataDir, compFName))
		if err != nil {
			slogger.Fatalf("delete file: %s error: %v\n", compFName, err)
		}
	}
}

// hint2MemIndex 从hint文件生成index
func hint2MemIndex(hint *Hint, fName string) *MemIdx {
	memIndex := &MemIdx{}
	memIndex.idxK = hint.key
	memIndex.idxV.tm = hint.tm
	memIndex.idxV.valsz = hint.valsz
	memIndex.idxV.valops = hint.valops
	memIndex.idxV.fName = fName
	return memIndex
}

// segment2MemIndex 从segment文件生成index
func segment2MemIndex(seg *Segment, fName string) *MemIdx {
	memIndex := &MemIdx{}
	memIndex.idxK = seg.key
	memIndex.idxV.fName = fName
	memIndex.idxV.tm = seg.tm
	memIndex.idxV.valsz = seg.valsz
	memIndex.idxV.valops = seg.valops
	return memIndex
}

// seg2Hint 从Segment 构造Hint
func seg2Hint(seg *Segment) *Hint {
	hint := &Hint{}
	hint.key = seg.key
	hint.keysz = seg.keysz
	hint.valsz = seg.valsz
	hint.valops = seg.valops
	hint.tm = seg.tm
	return hint
}

// hint2Seg 从Hint 构造Segment
func hint2Seg(hint *Hint) *Segment {
	seg := &Segment{}
	seg.tm = hint.tm
	seg.keysz = hint.keysz
	seg.valsz = hint.valsz
	seg.valops = hint.valops
	seg.key = hint.key
	return seg
}

// encodeHint 将数据按格式进行编码
func encodeHint(hint *Hint) string {
	return fmt.Sprintf(HintFormat, hint.tm, hint.keysz, hint.valsz, hint.valops, hint.key)
}

// decodeHint
func decodeHint(dataStr string) (*Hint, error) {
	hint := &Hint{}
	_, err := fmt.Sscanf(dataStr, HintFormat, &hint.tm, &hint.keysz, &hint.valsz, &hint.valops, &hint.key)
	if err != nil {
		return nil, fmt.Errorf("decodeHint seg2Hint: %s error: %v\n", dataStr, err)
	}
	return hint, nil
}

// encodeSeg 将数据按格式进行编码
func encodeSeg(seg *Segment) string {
	dataStr := fmt.Sprintf(SegFormat, seg.tm, seg.keysz, seg.valsz, seg.key, seg.value)
	checkSum := crc(dataStr)
	return fmt.Sprintf(CRCFormat, checkSum, dataStr)
}

// decodeSeg 从数据文件中解析数据
func decodeSeg(data string) (*Segment, error) {
	seg := &Segment{}
	var segKV, keyAndValue string
	_, err := fmt.Sscanf(data, CRCFormat, &seg.crcVal, &segKV)
	if err != nil {
		return nil, fmt.Errorf("decodeHint data: %s error: %v", data, err)
	}
	if !checkCRC(segKV, seg.crcVal) {
		return nil, fmt.Errorf("crc broken,data: %s,crc: %d", segKV, seg.crcVal)
	}
	_, err = fmt.Sscanf(segKV, SegFormatKV, &seg.tm, &seg.keysz, &seg.valsz, &keyAndValue)
	if err != nil {
		return nil, fmt.Errorf("segKV: %s error: %v", segKV, err)
	}
	seg.key = keyAndValue[:seg.keysz]
	seg.value = keyAndValue[seg.keysz:]
	return seg, nil
}

// seekKey 从段文件中读取key对应的value
func seekKey(dataDir string, index MemIdxV) (string, error) {
	f, err := os.OpenFile(path.Join(dataDir, index.fName), os.O_RDONLY, FileMode)
	defer f.Close()
	if err != nil {
		return "", fmt.Errorf("open file: %s error: %v", index.fName, err)
	}
	_, err = f.Seek(index.valops, 0)
	if err != nil {
		return "", fmt.Errorf("seek offset error: %v", err)
	}
	buf := make([]byte, index.valsz)
	_, err = io.ReadFull(f, buf)
	if err != nil {
		return "", fmt.Errorf("read file error: %v", err)
	}
	return string(buf), nil
}
