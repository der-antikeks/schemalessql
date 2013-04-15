package schemalessql_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"testing"
)

type P struct {
	Name    string
	X, Y, Z int
}

type Q struct {
	Name    string
	X, Y, Z float64
}

type R struct {
	Parent    P
	Childrens []Q
}

var (
	sampleInt     = P{"p", 123, 456, 789}
	sampleFloat   = Q{"q", 123.123, 456.456, 789.789}
	sampleComplex = R{P{"p", 123, 456, 789}, []Q{
		Q{"q", 123.123, 456.456, 789.789},
		Q{"q", 123.123, 456.456, 789.789},
	}}
)

func BenchmarkEncodeJsonInt(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	enc := json.NewEncoder(&buffer)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(sampleInt); err != nil {
			b.Fatal("encode error:", err)
		}
	}
}

func BenchmarkEncodeGobInt(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(sampleInt); err != nil {
			b.Fatal("encode error:", err)
		}
	}
}

func BenchmarkEncodeJsonFloat(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	enc := json.NewEncoder(&buffer)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(sampleFloat); err != nil {
			b.Fatal("encode error:", err)
		}
	}
}

func BenchmarkEncodeGobFloat(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(sampleFloat); err != nil {
			b.Fatal("encode error:", err)
		}
	}
}

func BenchmarkEncodeJsonComplex(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	enc := json.NewEncoder(&buffer)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(sampleComplex); err != nil {
			b.Fatal("encode error:", err)
		}
	}
}

func BenchmarkEncodeGobComplex(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(sampleComplex); err != nil {
			b.Fatal("encode error:", err)
		}
	}
}
