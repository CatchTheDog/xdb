package xdb

import "time"

const (
	DataDir           = "/Users/majunqiang/Documents/mrxdbengine/data/" // 段数据文件存放目录默认值
	SegFNamePrefix    = "seg"                                           // 段数据文件名称前缀
	HintFNamePrefix   = "hint"                                          // seg2Hint 文件名称前缀
	Delimiter         = "_"                                             // 文件名分隔符
	FileMode          = 0777                                            // 文件权限
	SegFormat         = "%016x%02x%03x%s%s"                             // 段文件数据格式
	CRCFormat         = "%08x%s\n"                                      // 段文件数据头部增加了CRC校验值的格式
	SegFormatKV       = "%016x%02x%03x%s\n"                             // 段文件数据头部增加了CRC校验值的数据，将key和value合并为一个字符串的格式
	NewLineSize       = len("\n")                                       // 字符串\n len
	SegSizeLimit      = 1 * 1024 * 1024                                 // 段文件size最大值：1MB
	SegFNameFormat    = "%3s_%d"                                        // 数据文件名称格式
	HintFNameFormat   = "%4s_%d"                                        // hint文件名称格式
	DataFNameFormat   = "%s_%d"                                         // 文件名称格式
	DataDelimiterByte = '\n'                                            // 数据分隔符
	SegFIDGap         = -50 * 365 * 24 * 3600 * time.Second             // 合并段文件ID与当前时间差值 -50年
	HintFormat        = "%016x%02x%03x%016x%s\n"                        // hint文件数据格式
	ASC               = 0                                               // 顺序
	DESC              = 1                                               // 倒序
	MaxSegmentNum     = 3                                               // 如果当前有超过MaxSegmentNum个冻结的段文件,就触发段合并，否则不进行段合并
	HTTPPort          = 8088                                            // http 请求端口
)
