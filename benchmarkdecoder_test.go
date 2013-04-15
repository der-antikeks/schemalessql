package schemalessql_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"testing"
)

func BenchmarkDecodeJsonInt(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	dec := json.NewDecoder(&buffer)

	enc := json.NewEncoder(&buffer)
	enc.Encode(sampleInt)

	var result P

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			b.Fatal("decode error:", err)
		}
	}
}

func BenchmarkDecodeGobInt(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	dec := gob.NewDecoder(&buffer)

	enc := gob.NewEncoder(&buffer)
	enc.Encode(sampleInt)

	var result P

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			b.Fatal("decode error:", err)
		}
	}
}

func BenchmarkDecodeJsonFloat(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	dec := json.NewDecoder(&buffer)

	enc := json.NewEncoder(&buffer)
	enc.Encode(sampleFloat)

	var result Q

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			b.Fatal("decode error:", err)
		}
	}
}

func BenchmarkDecodeGobFloat(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	dec := gob.NewDecoder(&buffer)

	enc := gob.NewEncoder(&buffer)
	enc.Encode(sampleFloat)

	var result Q

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			b.Fatal("decode error:", err)
		}
	}
}

func BenchmarkDecodeJsonComplex(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	dec := json.NewDecoder(&buffer)

	enc := json.NewEncoder(&buffer)
	enc.Encode(sampleComplex)

	var result R

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			b.Fatal("decode error:", err)
		}
	}
}

func BenchmarkDecodeGobComplex(b *testing.B) {
	b.StopTimer()
	var buffer bytes.Buffer
	dec := gob.NewDecoder(&buffer)

	enc := gob.NewEncoder(&buffer)
	enc.Encode(sampleComplex)

	var result R

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := dec.Decode(&result); err != nil && err != io.EOF {
			b.Fatal("decode error:", err)
		}
	}
}
