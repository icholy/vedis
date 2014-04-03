package vedis

import (
	"bytes"
	"errors"
	"reflect"
	"unsafe"
)

/*
#include <stdlib.h>
#include "vedis.h"

extern int InvokeFetchCallback(void*, void*, int);

static int
fetch_callback(const void* pData, unsigned int iDataLen, void *pUserData) {
  return InvokeFetchCallback(pUserData, (void*)pData, iDataLen);
}

static int
vedis_kv_fetch_invoke_callback(vedis *pStore, const void *pKey, int nKeyLen, void *pUserData) {
  return vedis_kv_fetch_callback(pStore, pKey, nKeyLen, &fetch_callback, pUserData);
}

static void
get_last_error_from_log(vedis *pStore, char *pDest) {
  int iLen = 0;
  vedis_config(pStore, VEDIS_CONFIG_ERR_LOG, &pDest, &iLen);
}

*/
import "C"

var (
	ErrReadOnly       = errors.New("Read-only database")
	ErrBusy           = errors.New("Another thread or process has an exclusive lock on the database")
	ErrIO             = errors.New("Input/Output error")
	ErrNoMemory       = errors.New("Out of Memory")
	ErrAbort          = errors.New("Another thread has released the database handle")
	ErrNotImplemented = errors.New("The underlying KV storage engine does not implement the xReplace() method")
	ErrPermission     = errors.New("Permission error")
	ErrLimit          = errors.New("Journal file record limit reached")
	ErrNotFound       = errors.New("Nonexistent record")
	ErrUnknown        = errors.New("Unknown error")
)

func getError(cErr C.int) error {
	switch cErr {
	case C.VEDIS_READ_ONLY:
		return ErrReadOnly
	case C.VEDIS_BUSY:
		return ErrBusy
	case C.VEDIS_IOERR:
		return ErrIO
	case C.VEDIS_NOMEM:
		return ErrNoMemory
	case C.VEDIS_ABORT:
		return ErrAbort
	case C.VEDIS_NOTIMPLEMENTED:
		return ErrNotImplemented
	case C.VEDIS_PERM:
		return ErrPermission
	case C.VEDIS_LIMIT:
		return ErrLimit
	case C.VEDIS_NOTFOUND:
		return ErrNotFound
	default:
		return ErrUnknown
	}
}

func Init()             { C.vedis_lib_init() }
func Shutdown()         { C.vedis_lib_shutdown() }
func Version() string   { return C.GoString(C.vedis_lib_version()) }
func Signature() string { return C.GoString(C.vedis_lib_signature()) }
func Identity() string  { return C.GoString(C.vedis_lib_ident()) }
func Copyright() string { return C.GoString(C.vedis_lib_copyright()) }

type Value struct{ p *C.vedis_value }

func (v *Value) String() string {
	var csLen C.int
	return C.GoString(C.vedis_value_to_string(v.p, &csLen))
}

func (v *Value) Int() int           { return int(C.vedis_value_to_int(v.p)) }
func (v *Value) Int64() int64       { return int64(C.vedis_value_to_int64(v.p)) }
func (v *Value) Bool() bool         { return C.vedis_value_to_bool(v.p) != 0 }
func (v *Value) Float64() float64   { return float64(C.vedis_value_to_double(v.p)) }
func (v *Value) Array() *ArrayValue { return &ArrayValue{v.p} }

func (v *Value) IsInt() bool     { return C.vedis_value_is_int(v.p) != 0 }
func (v *Value) IsFloat() bool   { return C.vedis_value_is_float(v.p) != 0 }
func (v *Value) IsBool() bool    { return C.vedis_value_is_bool(v.p) != 0 }
func (v *Value) IsString() bool  { return C.vedis_value_is_string(v.p) != 0 }
func (v *Value) IsNull() bool    { return C.vedis_value_is_null(v.p) != 0 }
func (v *Value) IsNumeric() bool { return C.vedis_value_is_numeric(v.p) != 0 }
func (v *Value) IsScalar() bool  { return C.vedis_value_is_scalar(v.p) != 0 }
func (v *Value) IsArray() bool   { return C.vedis_value_is_array(v.p) != 0 }

type ArrayValue Value

func (a *ArrayValue) Next() *Value {
	var value Value
	value.p = C.vedis_array_next_elem(a.p)
	if value.p == nil {
		return nil
	}
	return &value
}

func (a *ArrayValue) Index(i int) *Value {
	var value Value
	value.p = C.vedis_array_fetch(a.p, C.uint(i))
	if value.p == nil {
		panic("index out of range")
	}
	return &value
}

func (a *ArrayValue) Insert(val *Value) error {
	if rc := C.vedis_array_insert(a.p, val.p); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (a *ArrayValue) Count() int {
	return int(C.vedis_array_count(a.p))
}

func (a *ArrayValue) Reset() error {
	if rc := C.vedis_array_reset(a.p); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

type Store struct{ p *C.vedis }

func Open(name string) (*Store, error) {
	var store Store
	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))
	rc := C.vedis_open(&store.p, c_name)
	if rc != C.VEDIS_OK {
		return nil, getError(rc)
	}
	return &store, nil
}

func (s *Store) Close() { C.vedis_close(s.p) }

func (s *Store) Begin() error {
	if rc := C.vedis_begin(s.p); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (s *Store) Commit() error {
	if rc := C.vedis_commit(s.p); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (s *Store) Rollback() error {
	if rc := C.vedis_rollback(s.p); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (s *Store) Exec(cmd string) error {
	c_cmd := C.CString(cmd)
	defer C.free(unsafe.Pointer(c_cmd))
	rc := C.vedis_exec(s.p, c_cmd, -1)
	if rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (s *Store) ExecResult(cmd string) (*Value, error) {
	if err := s.Exec(cmd); err != nil {
		return nil, err
	}
	return s.Result()
}

func (s *Store) Result() (*Value, error) {
	var value Value
	C.vedis_exec_result(s.p, &value.p)
	return &value, nil
}

func prepareByteSlice(b []byte) (unsafe.Pointer, int) {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return unsafe.Pointer(header.Data), header.Len
}

func (s *Store) KvStore(key, val []byte) error {
	pKeyData, pKeyLen := prepareByteSlice(key)
	pValData, pValLen := prepareByteSlice(val)
	if rc := C.vedis_kv_store(
		s.p,
		pKeyData, C.int(pKeyLen),
		pValData, C.vedis_int64(pValLen),
	); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

//export InvokeFetchCallback
func InvokeFetchCallback(pFn, pData unsafe.Pointer, iLen C.int) C.int {
	fn := *(*func(unsafe.Pointer, C.int))(pFn)
	fn(pData, iLen)
	return C.VEDIS_OK
}

func (s *Store) KvFetch(key []byte) ([]byte, error) {
	pKeyData, pKeyLen := prepareByteSlice(key)
	var buffer bytes.Buffer
	pUserData := func(pData unsafe.Pointer, iLen C.int) {
		buffer.Write(C.GoBytes(pData, iLen))
	}
	if rc := C.vedis_kv_fetch_invoke_callback(
		s.p,
		pKeyData, C.int(pKeyLen),
		unsafe.Pointer(&pUserData),
	); rc != C.VEDIS_OK {
		return nil, getError(rc)
	}
	return buffer.Bytes(), nil
}

func (s *Store) KvAppend(key, val []byte) error {
	pKeyData, pKeyLen := prepareByteSlice(key)
	pValData, pValLen := prepareByteSlice(val)
	if rc := C.vedis_kv_append(
		s.p,
		pKeyData, C.int(pKeyLen),
		pValData, C.vedis_int64(pValLen),
	); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (s *Store) KvDelete(key []byte) error {
	pKeyData, pKeyLen := prepareByteSlice(key)
	if rc := C.vedis_kv_delete(
		s.p, pKeyData, C.int(pKeyLen),
	); rc != C.VEDIS_OK {
		return getError(rc)
	}
	return nil
}

func (s *Store) LastError() error {
	var msg *C.char
	C.get_last_error_from_log(s.p, msg)
	defer C.free(unsafe.Pointer(msg))
	return errors.New(C.GoString(msg))
}
