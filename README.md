# VEDIS

> CGO bindings to vedis

### Docs

* [godoc](http://godoc.org/github.com/icholy/vedis)  
* [vedis](http://vedis.symisc.net/)

### Install

``` sh
$ go get github.com/icholy/vedis
```

### Example

``` go
package main

import "github.com/icholy/vedis"
import "fmt"

func main() {

  // connect
  store, _ := Open(":mem:")
  defer store.Close()

  // set key: x value: 123
  _ = store.Exec("SET x 123")

  // get x
  result, _ := store.ExecResult("GET x")

  // display it
  fmt.Println("x:", result.Int())
}
```

