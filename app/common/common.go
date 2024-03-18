package common

import (
	"bufio"
	"io"
	"math/rand"
)

var alphaNumeric = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandomString(n int) string {
	b := make([]rune, n)
	for i := 0; i < n; i++ {
		b[i] = alphaNumeric[rand.Intn(len(alphaNumeric))]
	}
	return string(b)
}

func ReadWriterFrom(rw io.ReadWriter) *bufio.ReadWriter {
	r := bufio.NewReader(rw)
	w := bufio.NewWriter(rw)
	return bufio.NewReadWriter(r, w)
}
