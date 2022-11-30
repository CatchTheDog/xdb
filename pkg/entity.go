package pkg

// val 内嵌最小共用字段
type val struct {
	valsz  int   // 值长度
	valops int64 // 值在文件中的位置
	tm     int64 // 时间戳
}

// MemIdxV 表示内存索引
type MemIdxV struct {
	fName string // 文件名称
	val
}

// MemIdx 内存索引<idxK,value>对
type MemIdx struct {
	idxK string  // idxK
	idxV MemIdxV // 索引值
}

// Hint 表示hint file 文件数据
type Hint struct {
	key   string // 数据key
	keysz int    // 数据key长度
	val
}

// Segment 表示段文件数据
type Segment struct {
	value  string // 数据value
	crcVal uint32 // crc校验值
	Hint
}
