// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package memdb

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"
	"time"
)

type TestObject struct {
	ID       string
	Foo      string
	Fu       *string
	Boo      *string
	Bar      int
	Baz      string
	Bam      *bool
	Empty    string
	Qux      []string
	QuxEmpty []string
	Zod      map[string]string
	ZodEmpty map[string]string
	Int      int
	Int8     int8
	Int16    int16
	Int32    int32
	Int64    int64
	Uint     uint
	Uint8    uint8
	Uint16   uint16
	Uint32   uint32
	Uint64   uint64
	Bool     bool
}

func String(s string) *string {
	return &s
}

func testObj() *TestObject {
	b := true
	obj := &TestObject{
		ID:  "my-cool-obj",
		Foo: "Testing",
		Fu:  String("Fu"),
		Boo: nil,
		Bar: 42,
		Baz: "yep",
		Bam: &b,
		Qux: []string{"Test", "Test2"},
		Zod: map[string]string{
			"Role":          "Server",
			"instance_type": "m3.medium",
			"":              "asdf",
		},
		Int:    int(1),
		Int8:   int8(-1 << 7),
		Int16:  int16(-1 << 15),
		Int32:  int32(-1 << 31),
		Int64:  int64(-1 << 63),
		Uint:   uint(1),
		Uint8:  uint8(1<<8 - 1),
		Uint16: uint16(1<<16 - 1),
		Uint32: uint32(1<<32 - 1),
		Uint64: uint64(1<<64 - 1),
		Bool:   false,
	}
	return obj
}
func testObjUUID() *TestObject {
	b := true
	foo, _ := uuid.GenerateUUID()
	obj := &TestObject{
		ID:  "my-cool-obj",
		Foo: foo,
		Fu:  String("Fu"),
		Boo: nil,
		Bar: 42,
		Baz: "yep",
		Bam: &b,
		Qux: []string{"Test", "Test2"},
		Zod: map[string]string{
			"Role":          "Server",
			"instance_type": "m3.medium",
			"":              "asdf",
		},
		Int:    int(1),
		Int8:   int8(-1 << 7),
		Int16:  int16(-1 << 15),
		Int32:  int32(-1 << 31),
		Int64:  int64(-1 << 63),
		Uint:   uint(1),
		Uint8:  uint8(1<<8 - 1),
		Uint16: uint16(1<<16 - 1),
		Uint32: uint32(1<<32 - 1),
		Uint64: uint64(1<<64 - 1),
		Bool:   false,
	}
	return obj
}

func TestStringFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := StringFieldIndex{"Foo", false}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "Testing\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringFieldIndex{"Foo", true}
	ok, val, err = lower.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "testing\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringFieldIndex{"NA", true}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringFieldIndex{"Empty", true}
	ok, val, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}

	pointerField := StringFieldIndex{"Fu", false}
	ok, val, err = pointerField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "Fu\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	pointerField = StringFieldIndex{"Boo", false}
	ok, val, err = pointerField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "" {
		t.Fatalf("bad: %s", val)
	}
	if ok {
		t.Fatalf("should be not ok")
	}
}

func TestStringFieldIndex_FromArgs(t *testing.T) {
	indexer := StringFieldIndex{"Foo", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}

	lower := StringFieldIndex{"Foo", true}
	val, err = lower.FromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}
}

func TestStringFieldIndex_PrefixFromArgs(t *testing.T) {
	indexer := StringFieldIndex{"Foo", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.PrefixFromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.PrefixFromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}

	lower := StringFieldIndex{"Foo", true}
	val, err = lower.PrefixFromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}
}

func TestStringSliceFieldIndex_FromObject(t *testing.T) {
	obj := testObj()

	indexer := StringSliceFieldIndex{"Qux", false}
	ok, vals, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "Test\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "Test2\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringSliceFieldIndex{"Qux", true}
	ok, vals, err = lower.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "test\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "test2\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringSliceFieldIndex{"NA", true}
	ok, vals, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringSliceFieldIndex{"QuxEmpty", true}
	ok, vals, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestStringSliceFieldIndex_FromArgs(t *testing.T) {
	indexer := StringSliceFieldIndex{"Qux", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}

	lower := StringSliceFieldIndex{"Qux", true}
	val, err = lower.FromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00" {
		t.Fatalf("foo")
	}
}

func TestStringSliceFieldIndex_PrefixFromArgs(t *testing.T) {
	indexer := StringSliceFieldIndex{"Qux", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.PrefixFromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.PrefixFromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}

	lower := StringSliceFieldIndex{"Qux", true}
	val, err = lower.PrefixFromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}
}

func TestStringMapFieldIndex_FromObject(t *testing.T) {
	// Helper function to put the result in a deterministic order
	fromObjectSorted := func(index MultiIndexer, obj *TestObject) (bool, []string, error) {
		ok, v, err := index.FromObject(obj)
		var vals []string
		for _, s := range v {
			vals = append(vals, string(s))
		}
		sort.Strings(vals)
		return ok, vals, err
	}

	obj := testObj()

	indexer := StringMapFieldIndex{"Zod", false}
	ok, vals, err := fromObjectSorted(&indexer, obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatalf("bad result length of %d", len(vals))
	}
	if string(vals[0]) != "Role\x00Server\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "instance_type\x00m3.medium\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringMapFieldIndex{"Zod", true}
	ok, vals, err = fromObjectSorted(&lower, obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(vals) != 2 {
		t.Fatal("bad result length")
	}
	if string(vals[0]) != "instance_type\x00m3.medium\x00" {
		t.Fatalf("bad: %s", vals[0])
	}
	if string(vals[1]) != "role\x00server\x00" {
		t.Fatalf("bad: %s", vals[1])
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringMapFieldIndex{"NA", true}
	ok, _, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringMapFieldIndex{"ZodEmpty", true}
	ok, _, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestStringMapFieldIndex_FromArgs(t *testing.T) {
	indexer := StringMapFieldIndex{"Zod", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("Role", "Server")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "Role\x00Server\x00" {
		t.Fatalf("bad: %v", string(val))
	}

	lower := StringMapFieldIndex{"Zod", true}
	val, err = lower.FromArgs("Role", "Server")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "role\x00server\x00" {
		t.Fatalf("bad: %v", string(val))
	}
}

func TestUUIDFeldIndex_parseString(t *testing.T) {
	u := &UUIDFieldIndex{}
	_, err := u.parseString("invalid", true)
	if err == nil {
		t.Fatalf("should error")
	}

	buf, uuid := generateUUID()

	out, err := u.parseString(uuid, true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatalf("bad: %#v %#v", out, buf)
	}

	_, err = u.parseString("1-2-3-4-5-6", false)
	if err == nil {
		t.Fatalf("should error")
	}

	// Parse an empty string.
	out, err = u.parseString("", false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected := []byte{}
	if !bytes.Equal(out, expected) {
		t.Fatalf("bad: %#v %#v", out, expected)
	}

	// Parse an odd length UUID.
	input := "f23"
	out, err = u.parseString(input, false)
	if err == nil {
		t.Fatalf("expect error")
	}

	// Parse an even length UUID with hyphen.
	input = "20d8c509-3940-"
	out, err = u.parseString(input, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected = []byte{0x20, 0xd8, 0xc5, 0x09, 0x39, 0x40}
	if !bytes.Equal(out, expected) {
		t.Fatalf("bad: %#v %#v", out, expected)
	}
}

func TestUUIDFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	uuidBuf, uuid := generateUUID()
	obj.Foo = uuid
	indexer := &UUIDFieldIndex{"Foo"}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := &UUIDFieldIndex{"NA"}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := &UUIDFieldIndex{"Empty"}
	ok, val, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestUUIDFieldIndex_FromArgs(t *testing.T) {
	indexer := &UUIDFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	uuidBuf, uuid := generateUUID()

	val, err := indexer.FromArgs(uuid)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	val, err = indexer.FromArgs(uuidBuf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}
}

func TestUUIDFieldIndex_PrefixFromArgs(t *testing.T) {
	indexer := UUIDFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.PrefixFromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	uuidBuf, uuid := generateUUID()

	// Test full length.
	val, err := indexer.PrefixFromArgs(uuid)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	val, err = indexer.PrefixFromArgs(uuidBuf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	// Test partial.
	val, err = indexer.PrefixFromArgs(uuid[:6])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf[:3], val) {
		t.Fatalf("PrefixFromArgs returned %#v;\nwant %#v", val, uuidBuf[:3])
	}

	val, err = indexer.PrefixFromArgs(uuidBuf[:9])
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf[:9], val) {
		t.Fatalf("foo")
	}
}

func BenchmarkUUIDFieldIndex_parseString(b *testing.B) {
	_, uuid := generateUUID()
	indexer := &UUIDFieldIndex{}
	for i := 0; i < b.N; i++ {
		_, err := indexer.parseString(uuid, true)
		if err != nil {
			b.FailNow()
		}
	}
}

func generateUUID() ([]byte, string) {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
	return buf, uuid
}

func TestIntFieldIndex_FromObject(t *testing.T) {
	obj := testObj()

	eint := make([]byte, 8)
	eint8 := make([]byte, 1)
	eint16 := make([]byte, 2)
	eint32 := make([]byte, 4)
	eint64 := make([]byte, 8)
	binary.BigEndian.PutUint64(eint, 1<<63+1)
	eint8[0] = 0
	binary.BigEndian.PutUint16(eint16, 0)
	binary.BigEndian.PutUint32(eint32, 0)
	binary.BigEndian.PutUint64(eint64, 0)

	cases := []struct {
		Field         string
		Expected      []byte
		ErrorContains string
	}{
		{
			Field:    "Int",
			Expected: eint,
		},
		{
			Field:    "Int8",
			Expected: eint8,
		},
		{
			Field:    "Int16",
			Expected: eint16,
		},
		{
			Field:    "Int32",
			Expected: eint32,
		},
		{
			Field:    "Int64",
			Expected: eint64,
		},
		{
			Field:         "IntGarbage",
			ErrorContains: "is invalid",
		},
		{
			Field:         "ID",
			ErrorContains: "want an int",
		},
	}

	for _, c := range cases {
		t.Run(c.Field, func(t *testing.T) {
			indexer := IntFieldIndex{c.Field}
			ok, val, err := indexer.FromObject(obj)
			if err != nil {
				if ok {
					t.Fatalf("okay and error")
				}

				if c.ErrorContains != "" && strings.Contains(err.Error(), c.ErrorContains) {
					return
				} else {
					t.Fatalf("Unexpected error %v", err)
				}
			}

			if !ok {
				t.Fatalf("not okay and no error")
			}

			if !bytes.Equal(val, c.Expected) {
				t.Fatalf("bad: %#v %#v", val, c.Expected)
			}
		})
	}
}

func TestIntFieldIndex_FromArgs(t *testing.T) {
	indexer := IntFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(int(1), int(2))
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs("foo")
	if err == nil {
		t.Fatalf("should get err")
	}

	obj := testObj()
	eint := make([]byte, 8)
	eint8 := make([]byte, 1)
	eint16 := make([]byte, 2)
	eint32 := make([]byte, 4)
	eint64 := make([]byte, 8)
	binary.BigEndian.PutUint64(eint, 1<<63+1)
	eint8[0] = 0
	binary.BigEndian.PutUint16(eint16, 0)
	binary.BigEndian.PutUint32(eint32, 0)
	binary.BigEndian.PutUint64(eint64, 0)

	val, err := indexer.FromArgs(obj.Int)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, eint) {
		t.Fatalf("bad: %#v %#v", val, eint)
	}

	val, err = indexer.FromArgs(obj.Int8)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, eint8) {
		t.Fatalf("bad: %#v %#v", val, eint8)
	}

	val, err = indexer.FromArgs(obj.Int16)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, eint16) {
		t.Fatalf("bad: %#v %#v", val, eint16)
	}

	val, err = indexer.FromArgs(obj.Int32)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, eint32) {
		t.Fatalf("bad: %#v %#v", val, eint32)
	}

	val, err = indexer.FromArgs(obj.Int64)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, eint64) {
		t.Fatalf("bad: %#v %#v", val, eint64)
	}
}

func TestIntFieldIndexSortability(t *testing.T) {
	testCases := []struct {
		i8l      int8
		i8r      int8
		i16l     int16
		i16r     int16
		i32l     int32
		i32r     int32
		i64l     int64
		i64r     int64
		il       int
		ir       int
		expected int
		name     string
	}{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "zero"},
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, "small eq"},
		{0, 1, 0, 1, 0, 1, 0, 1, 0, 1, -1, "small lt"},
		{2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 1, "small gt"},
		{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, "small neg eq"},
		{-2, -1, -2, -1, -2, -1, -2, -1, -2, -1, -1, "small neg lt"},
		{-1, -2, -1, -2, -1, -2, -1, -2, -1, -2, 1, "small neg gt"},
		{-1, 1, -1, 1, -1, 1, -1, 1, -1, 1, -1, "neg vs pos"},
		{-128, 127, -32768, 32767, -2147483648, 2147483647, -9223372036854775808, 9223372036854775807, -9223372036854775808, 9223372036854775807, -1, "max conditions"},
		{100, 127, 1000, 2000, 1000000000, 2000000000, 10000000000, 20000000000, 1000000000, 2000000000, -1, "large lt"},
		{100, 99, 1000, 999, 1000000000, 999999999, 10000000000, 9999999999, 1000000000, 999999999, 1, "large gt"},
		{126, 127, 255, 256, 65535, 65536, 4294967295, 4294967296, 65535, 65536, -1, "edge conditions"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compareEncoded(t, &IntFieldIndex{"Foo"}, tc.i8l, tc.i8r, tc.expected)
			compareEncoded(t, &IntFieldIndex{"Foo"}, tc.i16l, tc.i16r, tc.expected)
			compareEncoded(t, &IntFieldIndex{"Foo"}, tc.i32l, tc.i32r, tc.expected)
			compareEncoded(t, &IntFieldIndex{"Foo"}, tc.i64l, tc.i64r, tc.expected)
			compareEncoded(t, &IntFieldIndex{"Foo"}, tc.il, tc.ir, tc.expected)
		})
	}
}

func TestUintFieldIndex_FromObject(t *testing.T) {
	obj := testObj()

	euint := make([]byte, 8)
	euint8 := make([]byte, 1)
	euint16 := make([]byte, 2)
	euint32 := make([]byte, 4)
	euint64 := make([]byte, 8)
	binary.BigEndian.PutUint64(euint, uint64(obj.Uint))
	euint8[0] = obj.Uint8
	binary.BigEndian.PutUint16(euint16, obj.Uint16)
	binary.BigEndian.PutUint32(euint32, obj.Uint32)
	binary.BigEndian.PutUint64(euint64, obj.Uint64)

	cases := []struct {
		Field         string
		Expected      []byte
		ErrorContains string
	}{
		{
			Field:    "Uint",
			Expected: euint,
		},
		{
			Field:    "Uint8",
			Expected: euint8,
		},
		{
			Field:    "Uint16",
			Expected: euint16,
		},
		{
			Field:    "Uint32",
			Expected: euint32,
		},
		{
			Field:    "Uint64",
			Expected: euint64,
		},
		{
			Field:         "UintGarbage",
			ErrorContains: "is invalid",
		},
		{
			Field:         "ID",
			ErrorContains: "want a uint",
		},
	}

	for _, c := range cases {
		t.Run(c.Field, func(t *testing.T) {
			indexer := UintFieldIndex{c.Field}
			ok, val, err := indexer.FromObject(obj)
			if err != nil {
				if ok {
					t.Fatalf("okay and error")
				}

				if c.ErrorContains != "" && strings.Contains(err.Error(), c.ErrorContains) {
					return
				} else {
					t.Fatalf("Unexpected error %v", err)
				}
			}

			if !ok {
				t.Fatalf("not okay and no error")
			}

			if !bytes.Equal(val, c.Expected) {
				t.Fatalf("bad: %#v %#v", val, c.Expected)
			}
		})
	}
}

func TestUintFieldIndex_FromArgs(t *testing.T) {
	indexer := UintFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(uint(1), uint(2))
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs("foo")
	if err == nil {
		t.Fatalf("should get err")
	}

	obj := testObj()
	euint := make([]byte, 8)
	euint8 := make([]byte, 1)
	euint16 := make([]byte, 2)
	euint32 := make([]byte, 4)
	euint64 := make([]byte, 8)
	binary.BigEndian.PutUint64(euint, uint64(obj.Uint))
	euint8[0] = obj.Uint8
	binary.BigEndian.PutUint16(euint16, obj.Uint16)
	binary.BigEndian.PutUint32(euint32, obj.Uint32)
	binary.BigEndian.PutUint64(euint64, obj.Uint64)

	val, err := indexer.FromArgs(obj.Uint)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint) {
		t.Fatalf("bad: %#v %#v", val, euint)
	}

	val, err = indexer.FromArgs(obj.Uint8)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint8) {
		t.Fatalf("bad: %#v %#v", val, euint8)
	}

	val, err = indexer.FromArgs(obj.Uint16)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint16) {
		t.Fatalf("bad: %#v %#v", val, euint16)
	}

	val, err = indexer.FromArgs(obj.Uint32)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint32) {
		t.Fatalf("bad: %#v %#v", val, euint32)
	}

	val, err = indexer.FromArgs(obj.Uint64)
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	if !bytes.Equal(val, euint64) {
		t.Fatalf("bad: %#v %#v", val, euint64)
	}
}

func TestUIntFieldIndexSortability(t *testing.T) {
	testCases := []struct {
		u8l      uint8
		u8r      uint8
		u16l     uint16
		u16r     uint16
		u32l     uint32
		u32r     uint32
		u64l     uint64
		u64r     uint64
		ul       uint
		ur       uint
		expected int
		name     string
	}{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "zero"},
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, "small eq"},
		{0, 1, 0, 1, 0, 1, 0, 1, 0, 1, -1, "small lt"},
		{2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 1, "small gt"},
		{100, 200, 1000, 2000, 1000000000, 2000000000, 10000000000, 20000000000, 1000000000, 2000000000, -1, "large lt"},
		{100, 99, 1000, 999, 1000000000, 999999999, 10000000000, 9999999999, 1000000000, 999999999, 1, "large gt"},
		{127, 128, 255, 256, 65535, 65536, 4294967295, 4294967296, 65535, 65536, -1, "edge conditions"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compareEncoded(t, &UintFieldIndex{"Foo"}, tc.u8l, tc.u8r, tc.expected)
			compareEncoded(t, &UintFieldIndex{"Foo"}, tc.u16l, tc.u16r, tc.expected)
			compareEncoded(t, &UintFieldIndex{"Foo"}, tc.u32l, tc.u32r, tc.expected)
			compareEncoded(t, &UintFieldIndex{"Foo"}, tc.u64l, tc.u64r, tc.expected)
			compareEncoded(t, &UintFieldIndex{"Foo"}, tc.ul, tc.ur, tc.expected)
		})
	}
}

func compareEncoded(t *testing.T, indexer Indexer, l interface{}, r interface{}, expected int) {
	lBytes, err := indexer.FromArgs(l)
	if err != nil {
		t.Fatalf("unable to encode %d: %s", l, err)
	}
	rBytes, err := indexer.FromArgs(r)
	if err != nil {
		t.Fatalf("unable to encode %d: %s", r, err)
	}

	if bytes.Compare(lBytes, rBytes) != expected {
		t.Fatalf("Compare(%#v, %#v) != %d", lBytes, rBytes, expected)
	}
}

func TestBoolFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := BoolFieldIndex{Field: "Bool"}

	obj.Bool = false
	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}

	obj.Bool = true
	ok, val, err = indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	indexer = BoolFieldIndex{Field: "NA"}
	ok, val, err = indexer.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	indexer = BoolFieldIndex{Field: "ID"}
	ok, val, err = indexer.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}
}

func TestBoolFieldIndex_FromArgs(t *testing.T) {
	indexer := BoolFieldIndex{Field: "Bool"}

	val, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err = indexer.FromArgs(true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	val, err = indexer.FromArgs(false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

func TestFieldSetIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := FieldSetIndex{"Bam"}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	emptyIndexer := FieldSetIndex{"Empty"}
	ok, val, err = emptyIndexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}

	setIndexer := FieldSetIndex{"Bar"}
	ok, val, err = setIndexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := FieldSetIndex{"NA"}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	obj.Bam = nil
	nilIndexer := FieldSetIndex{"Bam"}
	ok, val, err = nilIndexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

func TestFieldSetIndex_FromArgs(t *testing.T) {
	indexer := FieldSetIndex{"Bam"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs(true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	val, err = indexer.FromArgs(false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

// A conditional that checks if TestObject.Bar == 42
var conditional = func(obj interface{}) (bool, error) {
	test, ok := obj.(*TestObject)
	if !ok {
		return false, fmt.Errorf("Expect only TestObj types")
	}

	if test.Bar != 42 {
		return false, nil
	}

	return true, nil
}

func TestConditionalIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := ConditionalIndex{conditional}
	obj.Bar = 42
	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	// Change the object so it should return false.
	obj.Bar = 2
	ok, val, err = indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}

	// Pass an invalid type.
	ok, val, err = indexer.FromObject(t)
	if err == nil {
		t.Fatalf("expected an error when passing invalid type")
	}
}

func TestConditionalIndex_FromArgs(t *testing.T) {
	indexer := ConditionalIndex{conditional}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs(true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 1 {
		t.Fatalf("bad: %v", val)
	}

	val, err = indexer.FromArgs(false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 1 || val[0] != 0 {
		t.Fatalf("bad: %v", val)
	}
}

func TestCompoundIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := &CompoundIndex{
		Indexes: []Indexer{
			&StringFieldIndex{"ID", false},
			&StringFieldIndex{"Foo", false},
			&StringFieldIndex{"Baz", false},
		},
		AllowMissing: false,
	}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "my-cool-obj\x00Testing\x00yep\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	missing := &CompoundIndex{
		Indexes: []Indexer{
			&StringFieldIndex{"ID", false},
			&StringFieldIndex{"Foo", true},
			&StringFieldIndex{"Empty", false},
		},
		AllowMissing: true,
	}
	ok, val, err = missing.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "my-cool-obj\x00testing\x00" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	// Test when missing not allowed
	missing.AllowMissing = false
	ok, _, err = missing.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not be okay")
	}
}

func TestCompoundIndex_FromArgs(t *testing.T) {
	indexer := &CompoundIndex{
		Indexes: []Indexer{
			&StringFieldIndex{"ID", false},
			&StringFieldIndex{"Foo", false},
			&StringFieldIndex{"Baz", false},
		},
		AllowMissing: false,
	}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42, 42, 42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo", "bar", "baz")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo\x00bar\x00baz\x00" {
		t.Fatalf("bad: %s", val)
	}
}

func TestCompoundIndex_PrefixFromArgs(t *testing.T) {
	indexer := &CompoundIndex{
		Indexes: []Indexer{
			&UUIDFieldIndex{"ID"},
			&StringFieldIndex{"Foo", false},
			&StringFieldIndex{"Baz", false},
		},
		AllowMissing: false,
	}
	val, err := indexer.PrefixFromArgs()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(val) != 0 {
		t.Fatalf("bad: %s", val)
	}

	uuidBuf, uuid := generateUUID()
	val, err = indexer.PrefixFromArgs(uuid, "foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(val[:16], uuidBuf) {
		t.Fatalf("bad prefix")
	}
	if string(val[16:]) != "foo" {
		t.Fatalf("bad: %s", val)
	}

	val, err = indexer.PrefixFromArgs(uuid, "foo", "ba")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(val[:16], uuidBuf) {
		t.Fatalf("bad prefix")
	}
	if string(val[16:]) != "foo\x00ba" {
		t.Fatalf("bad: %s", val)
	}

	_, err = indexer.PrefixFromArgs(uuid, "foo", "bar", "nope")
	if err == nil {
		t.Fatalf("expected an error when passing too many arguments")
	}
}

func TestCompoundMultiIndex_FromObject(t *testing.T) {
	// handle sub-indexer case unique to MultiIndexer
	obj := &TestObject{
		ID:       "obj1-uuid",
		Foo:      "Foo1",
		Baz:      "yep",
		Qux:      []string{"Test", "Test2"},
		QuxEmpty: []string{"Qux", "Qux2"},
	}
	indexer := &CompoundMultiIndex{
		Indexes: []Indexer{
			&StringFieldIndex{Field: "Foo"},
			&StringSliceFieldIndex{Field: "Qux"},
			&StringSliceFieldIndex{Field: "QuxEmpty"},
		},
	}

	ok, vals, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("should be ok")
	}
	want := []string{
		"Foo1\x00Test\x00Qux\x00",
		"Foo1\x00Test\x00Qux2\x00",
		"Foo1\x00Test2\x00Qux\x00",
		"Foo1\x00Test2\x00Qux2\x00",
	}
	got := make([]string, len(vals))
	for i, v := range vals {
		got[i] = string(v)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("\ngot:  %+v\nwant: %+v\n", got, want)
	}
}

func TestCompoundMultiIndex_FromObject_IndexUniquenessProperty(t *testing.T) {
	indexPermutations := [][]string{
		{"Foo", "Qux", "QuxEmpty"},
		{"Foo", "QuxEmpty", "Qux"},
		{"QuxEmpty", "Qux", "Foo"},
		{"QuxEmpty", "Foo", "Qux"},
		{"Qux", "QuxEmpty", "Foo"},
		{"Qux", "Foo", "QuxEmpty"},
	}

	fn := func(o TestObject) bool {
		for _, perm := range indexPermutations {
			indexer := indexerFromFieldNameList(perm)
			ok, vals, err := indexer.FromObject(o)
			if err != nil {
				t.Logf("err: %v", err)
				return false
			}
			if !ok {
				t.Logf("should be ok")
				return false
			}
			if !assertAllUnique(t, vals) {
				return false
			}
		}
		return true
	}
	seed := time.Now().UnixNano()
	t.Logf("Using seed %v", seed)
	cfg := quick.Config{Rand: rand.New(rand.NewSource(seed))}
	if err := quick.Check(fn, &cfg); err != nil {
		t.Fatalf("property not held: %v", err)
	}
}

func assertAllUnique(t *testing.T, vals [][]byte) bool {
	t.Helper()
	s := make(map[string]struct{}, len(vals))
	for _, index := range vals {
		s[string(index)] = struct{}{}
	}

	if l := len(s); l != len(vals) {
		t.Logf("expected %d unique indexes, got %v", len(vals), l)
		return false
	}
	return true
}

func indexerFromFieldNameList(keys []string) *CompoundMultiIndex {
	indexer := &CompoundMultiIndex{AllowMissing: true}
	for _, key := range keys {
		if key == "Foo" || key == "Baz" {
			indexer.Indexes = append(indexer.Indexes, &StringFieldIndex{Field: key})
			continue
		}
		indexer.Indexes = append(indexer.Indexes, &StringSliceFieldIndex{Field: key})
	}
	return indexer
}

func BenchmarkCompoundMultiIndex_FromObject(b *testing.B) {
	obj := &TestObject{
		ID:       "obj1-uuid",
		Foo:      "Foo1",
		Baz:      "yep",
		Qux:      []string{"Test", "Test2"},
		QuxEmpty: []string{"Qux", "Qux2"},
	}
	indexer := &CompoundMultiIndex{
		Indexes: []Indexer{
			&StringFieldIndex{Field: "Foo"},
			&StringSliceFieldIndex{Field: "Qux"},
			&StringSliceFieldIndex{Field: "QuxEmpty"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, vals, err := indexer.FromObject(obj)
		if err != nil {
			b.Fatalf("expected no error, got: %v", err)
		}
		if !ok {
			b.Fatalf("should be ok")
		}
		if l := len(vals); l != 4 {
			b.Fatalf("expected 4 indexes, got %v", l)
		}
	}
}
