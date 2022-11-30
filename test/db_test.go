package test

import (
	"fmt"
	"net/http"
	"testing"
)

func BenchmarkDB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		putUrl := fmt.Sprintf("http://localhost:8088/put?key=%d&value=%d", i, i+1)
		getUrl := fmt.Sprintf("http://localhost:8088/get?key=%d", i)
		fmt.Printf("putUrl: %s, getUrl: %s\n", putUrl, getUrl)
		http.Get(putUrl)
	}
}
