package main

// import (
// 	"encoding/binary"
// 	"fmt"
// 	// "io/fs"
// 	"os"
// )
//
// const (
// 	PageSize = 8192
// )
//
// func main() {
// 	filename := "./page.bat"
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		file, err = os.Create(filename)
// 		if err != nil {
// 			fmt.Printf("❤❤❤ tuannm: [testfile.go][17][err]: %+v\n", err)
// 		}
// 		defer file.Close()
// 	}
//
// 	err = binary.Write(file, binary.LittleEndian, 100)
// 	if err != nil {
// 		fmt.Printf("❤❤❤ tuannm: [testfile.go][26][err]: %+v\n", err)
// 	}
//
// 	var test int
// 	err = binary.Read(file, binary.LittleEndian, test)
//
// 	if err != nil {
// 		fmt.Printf("❤❤❤ tuannm: [testfile.go][31][err]: %+v\n", err)
// 	}
// 	fmt.Printf("❤❤❤ tuannm: [testfile.go][30][test]: %+v\n", test)
// }
