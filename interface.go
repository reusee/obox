package obox

type Db interface {
  Set(key string, obj interface{}) (error)
  SetDefault(key string, obj interface{}) (set bool, err error)
  Get(key string, objPointer interface{}) (error)
  Iter(fun func(key string, get Getter) bool)
  Count() int64
  Close() error
}

type Getter func(objPointer interface{}) error
