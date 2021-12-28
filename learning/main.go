package main

import (
	"fmt"
	"github.com/hashicorp/go-memdb/minidb"
	"reflect"
)

func main() {
	person := &minidb.Person{Name: "fanyinghao", Age: 22, Email: "282445408@qq.com"}
	reflectTest(person)
}

func reflectTest(obj interface{}) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v)
	fv := v.FieldByName("Email")
	fv = reflect.Indirect(fv)
	fmt.Printf("%v", fv.String())
}
