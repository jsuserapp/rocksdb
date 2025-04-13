package ju

import (
	"fmt"
	"github.com/gookit/color"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	ColorRed     = "red"
	ColorGreen   = "green"
	ColorYellow  = "yellow"
	ColorBlack   = "black"
	ColorWhite   = "white"
	ColorMagenta = "magenta"
	ColorCyan    = "cyan"
	ColorBlue    = "blue"
)

func GetTrace(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown:0"
	}
	file = path.Base(file)
	return fmt.Sprintf("%s:%d", file, line)
}
func GetNowTimeMs() string {
	return time.Now().Format("15:04:05.000")
}

type colorPrint func(format string, a ...interface{})

// OutputColor 这个函数输出效果和logColor相同，但是只输出到控制台，任何时候都不会保存到数据库
// skip: 0 是OutputColor的调用位置, 1 是上一级函数的调用位置
// noinspection GoUnusedExportedFunction
func OutputColor(skip int, color string, v ...interface{}) {
	trace := GetTrace(skip + 2)
	var builder strings.Builder
	for i, value := range v {
		if i > 0 {
			builder.WriteString(" ")
		}
		builder.WriteString(fmt.Sprint(value))
	}
	str := builder.String()
	cp := getColorPrint(color)
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Print(GetNowTimeMs(), " ", trace, " ")
	cp("%s\n", str)
}

var logMutex sync.Mutex

func logColor(skip int, c string, v ...interface{}) {
	trace := GetTrace(skip)

	var builder strings.Builder
	for i, value := range v {
		if i > 0 {
			builder.WriteString(" ")
		}
		builder.WriteString(fmt.Sprint(value))
	}
	str := builder.String()
	cp := getColorPrint(c)
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Print(GetNowTimeMs(), " ", trace, " ")
	cp("%s\n", str)
}

func logColorF(skip int, c, format string, v ...interface{}) {
	trace := GetTrace(skip)
	cp := getColorPrint(c)
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Print(GetNowTimeMs(), " ", trace, " ")
	cp(format, v...)
}
func getColorPrint(c string) (cp colorPrint) {
	switch c {
	case ColorBlack:
		return color.Black.Printf
	case ColorWhite:
		return color.White.Printf
	case ColorGreen:
		return color.Green.Printf
	case ColorRed:
		return color.Red.Printf
	case ColorBlue:
		return color.Blue.Printf
	case ColorMagenta:
		return color.Magenta.Printf
	case ColorYellow:
		return color.Yellow.Printf
	case ColorCyan:
		return color.Cyan.Printf
	}
	return color.Black.Printf
}

// noinspection GoUnusedExportedFunction
func LogBlackF(format string, a ...interface{}) { logColorF(3, "black", format, a...) }

// noinspection GoUnusedExportedFunction
func LogRedF(format string, a ...interface{}) { logColorF(3, "red", format, a...) }

// noinspection GoUnusedExportedFunction
func LogGreenF(format string, a ...interface{}) { logColorF(3, "green", format, a...) }

// noinspection GoUnusedExportedFunction
func LogYellowF(format string, a ...interface{}) { logColorF(3, "yellow", format, a...) }

// noinspection GoUnusedExportedFunction
func LogBlueF(format string, a ...interface{}) { logColorF(3, "blue", format, a...) }

// noinspection GoUnusedExportedFunction
func LogMagentaF(format string, a ...interface{}) { logColorF(3, "magenta", format, a...) }

// noinspection GoUnusedExportedFunction
func LogCyanF(format string, a ...interface{}) { logColorF(3, "cyan", format, a...) }

// noinspection GoUnusedExportedFunction
func LogWhiteF(format string, a ...interface{}) { logColorF(3, "white", format, a...) }

// noinspection GoUnusedExportedFunction
func LogBlack(a ...interface{}) { logColor(3, "black", a...) }

// noinspection GoUnusedExportedFunction
func LogRed(a ...interface{}) { logColor(3, "red", a...) }

// noinspection GoUnusedExportedFunction
func LogGreen(a ...interface{}) { logColor(3, "green", a...) }

// noinspection GoUnusedExportedFunction
func LogYellow(a ...interface{}) { logColor(3, "yellow", a...) }

// noinspection GoUnusedExportedFunction
func LogBlue(a ...interface{}) { logColor(3, "blue", a...) }

// noinspection GoUnusedExportedFunction
func LogMagenta(a ...interface{}) { logColor(3, "magenta", a...) }

// noinspection GoUnusedExportedFunction
func LogCyan(a ...interface{}) { logColor(3, "cyan", a...) }

// noinspection GoUnusedExportedFunction
func LogWhite(a ...interface{}) { logColor(3, "white", a...) }
