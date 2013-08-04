package obox

/*
#include "sqlite4/lsm.h"
#include <stdlib.h>
#cgo LDFLAGS: -L./sqlite4 -lsqlite4 -lm
*/
import "C"
import (
  "errors"
  "unsafe"
  "fmt"
  "bytes"
  "reflect"
)

const (
  lsmTrue = C.int(1)
  lsmFalse = C.int(0)
)

type LsmDb struct {
  cdb *C.lsm_db
}

func OpenLsmDb(dbFile string) (*LsmDb, error) {
  var cdb *C.lsm_db;
  rc := C.lsm_new(nil, &cdb)
  if rc != C.LSM_OK {
    return nil, errors.New("cannot create lsm handle")
  }
  cDbFile := C.CString(dbFile)
  defer C.free(unsafe.Pointer(cDbFile))
  rc = C.lsm_open(cdb, cDbFile)
  if rc != C.LSM_OK {
    return nil, errors.New(fmt.Sprintf("cannot open database file %s", dbFile))
  }
  db := &LsmDb{
    cdb: cdb,
  }
  return db, nil
}

func (self *LsmDb) Close() error {
  rc := C.lsm_close(self.cdb)
  if rc != C.LSM_OK {
    return errors.New("error when closing the database")
  }
  return nil
}

func (self *LsmDb) Count() int64 {
  return 0 //TODO
}

func (self *LsmDb) Get(key string, obj interface{}) error {
  var cur *C.lsm_cursor;
  rc := C.lsm_csr_open(self.cdb, &cur)
  if rc != C.LSM_OK {
    return errors.New("cannot create cursor")
  }
  defer C.lsm_csr_close(cur)
  cKey := C.CString(key)
  defer C.free(unsafe.Pointer(cKey))
  rc = C.lsm_csr_seek(cur, unsafe.Pointer(cKey), C.int(len(key)), C.LSM_SEEK_EQ)
  if rc != C.LSM_OK {
    return errors.New("error when seeking a key")
  }
  if C.lsm_csr_valid(cur) == lsmTrue {
    var valP unsafe.Pointer
    var valL C.int
    rc = C.lsm_csr_value(cur, &valP, &valL)
    if rc != C.LSM_OK {
      return errors.New("error when retrieving value")
    }
    bs := C.GoBytes(valP, valL)
    r := bytes.NewReader(bs)
    err := decode(r, obj)
    if err != nil { return err }
  }
  return nil
}

func (self *LsmDb) Set(key string, obj interface{}) error {
  buf := new(bytes.Buffer)
  err := encode(buf, obj)
  if err != nil { return err }
  value := buf.Bytes()
  cKey := unsafe.Pointer(C.CString(key))
  defer C.free(cKey)
  header := (*reflect.SliceHeader)(unsafe.Pointer(&value))
  rc := C.lsm_insert(self.cdb, cKey, C.int(len(key)), unsafe.Pointer(header.Data), C.int(header.Len))
  if rc != C.LSM_OK {
    return errors.New("error when inserting")
  }
  return nil
}

func (self *LsmDb) Iter(fun func(string, Getter) bool) {
  var cur *C.lsm_cursor;
  rc := C.lsm_csr_open(self.cdb, &cur)
  if rc != C.LSM_OK {
    return
  }
  defer C.lsm_csr_close(cur)
  var keyP, valP unsafe.Pointer
  var keyL, valL C.int
  for rc := C.lsm_csr_first(cur); rc == C.LSM_OK && C.lsm_csr_valid(cur) == lsmTrue; rc = C.lsm_csr_next(cur) {
    rc = C.lsm_csr_key(cur, &keyP, &keyL)
    if rc != C.LSM_OK { break }
    rc = C.lsm_csr_value(cur, &valP, &valL)
    if rc != C.LSM_OK { break }
    key := string(C.GoBytes(keyP, keyL))
    value := C.GoBytes(valP, valL)
    r := bytes.NewReader(value)
    ret := fun(key, func(obj interface{}) error {
      return decode(r, obj)
    })
    if ret == false {
      break
    }
  }
}

func (self *LsmDb) SetDefault(key string, obj interface{}) (bool, error) {
  return false, nil //TODO
}
