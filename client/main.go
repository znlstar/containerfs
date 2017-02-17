package main

import (
	"fmt"
	fs "ipd.org/containerfs/fs"
	"os"
)

func main() {
	cfs := fs.OpenFileSystem("UUID1")
	switch os.Args[1] {

	case "createvol":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("createvol [volname] [space GB]")
			os.Exit(1)
		}
		fs.CreateVol(os.Args[2], os.Args[3])

	case "getvolinfo":
		argNum := len(os.Args)
		if argNum != 3 {
			fmt.Println("getvolinfo [volUUID]")
			os.Exit(1)
		}
		fs.GetVolInfo(os.Args[2])

	case "createdir":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("createdir [volUUID] [dirname]")
			os.Exit(1)
		}
		cfs.CreateDir(os.Args[3])

	case "stat":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("stat [volUUID] [dir/filename]")
			os.Exit(1)
		}
		cfs.Stat(os.Args[3])
	case "ls":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("ls [volUUID] [dir/filename]")
			os.Exit(1)
		}
		cfs.List(os.Args[3])
	case "ll":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("ll [volUUID] [dir/filename]")
			os.Exit(1)
		}
		cfs.ListAll(os.Args[3])
	case "deletedir":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("deletedir [volUUID] [dirname]")
			os.Exit(1)
		}
		cfs.DeleteDir(os.Args[3])
	case "mv":
		argNum := len(os.Args)
		if argNum != 5 {
			fmt.Println("mv [volUUID] [dirname1] [dirname2]")
			os.Exit(1)
		}
		cfs.Rename(os.Args[3], os.Args[4])
	case "touch":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("touch [volUUID] [filename]")
			os.Exit(1)
		}
		_, file := cfs.OpenFile(os.Args[3], fs.O_RDONLY)
		fmt.Println(file)

	}
}
