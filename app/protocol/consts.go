package protocol

import (
	"encoding/base64"
)

const emptyRDBFileBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

func getEmptyRDBFileBinary() string {
	b, err := base64.StdEncoding.DecodeString(emptyRDBFileBase64)
	if err != nil {
		panic("this should never error")
	}
	return string(b)
}
