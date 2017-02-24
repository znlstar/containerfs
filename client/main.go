package main

import (
	"bufio"
	"fmt"
	fs "ipd.org/containerfs/fs"
	"ipd.org/containerfs/utils"
	"os"
)

func main() {

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
		cfs := fs.OpenFileSystem(os.Args[2])
		cfs.CreateDir(os.Args[3])

	case "stat":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("stat [volUUID] [dir/filename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		cfs.Stat(os.Args[3])
	case "ls":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("ls [volUUID] [dir/filename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		cfs.List(os.Args[3])
	case "ll":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("ll [volUUID] [dir/filename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		cfs.ListAll(os.Args[3])
	case "deletedir":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("deletedir [volUUID] [dirname]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		cfs.DeleteDir(os.Args[3])
	case "mv":
		argNum := len(os.Args)
		if argNum != 5 {
			fmt.Println("mv [volUUID] [dirname1] [dirname2]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		cfs.Rename(os.Args[3], os.Args[4])
	case "touch":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("touch [volUUID] [filename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		_, file := cfs.OpenFile(os.Args[3], fs.O_WRONLY)
		fmt.Println(file)

	case "allocatechunk":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("allocatechunk [volUUID] [filename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		_, ack := cfs.AllocateChunk(os.Args[3])
		fmt.Println(ack)

	case "get":
		argNum := len(os.Args)
		if argNum != 5 {
			fmt.Println("get [voluuid] [cfsfilename] [dstfilename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		get(cfs, os.Args[3], os.Args[4])

	case "put":
		argNum := len(os.Args)
		if argNum != 5 {
			fmt.Println("put [volUUID] [localfilename] [cfsfilename]")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(os.Args[2])
		put(cfs, os.Args[3], os.Args[4])
	}

}

func get(cfs *fs.CFS, cfsFile string, dstFile string) {
	ret := cfs.Stat(cfsFile)
	if ret != 0 {
		fmt.Print("Get Bad FilePath from CFS!\n")
		os.Exit(1)
	}
	//if ok, _ := utils.LocalPathExists(dstFile); !ok {
	//	f, err := os.Create(dstFile)
	//}
	ret, cfile := cfs.OpenFile(cfsFile, fs.O_RDONLY)
	defer cfile.Close()

	f, err := os.Create(dstFile)
	if err != nil {
		fmt.Println("Open local dstFile error!\n")
		os.Exit(1)
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	buf := make([]byte, 1024*1024)
	var bytes int64 = 0
	var length int64 = 0

	for {
		length = cfile.Read(&buf, int64(len(buf)))
		if length <= 0 {
			if length < 0 {
				fmt.Println("Read from CFSFile fail!\n")
				os.Exit(1)
			}
			break
		}
		bytes += length
		//fmt.Printf("####### buflen:%v,  bufcap:%v ######\n",len(buf),cap(buf))
		if n, err := w.Write(buf); err != nil {
			fmt.Printf("Get from CFSfile to Localfile err:%v !\n", err)
			os.Exit(1)
		} else if int64(n) != length {
			fmt.Printf("Get from CFSfile to write Localfile incorrect, retsize:%v, writesize:%v !!!\n", length, n)
			os.Exit(1)
		}
	}
	if err = w.Flush(); err != nil {
		fmt.Println("Flush Localfile data err!\n")
		os.Exit(1)
	}

	fmt.Printf("#### Read %v bytes from %s have finised!\n", bytes, cfsFile)
}

func put(cfs *fs.CFS, localFile string, cfsFile string) {
	if ok, _ := utils.LocalPathExists(localFile); !ok {
		fmt.Println("local file not exist!")
		os.Exit(1)
	}
	_, cfile := cfs.OpenFile(cfsFile, fs.O_WRONLY)
	fs.ReadLocalAndWriteCFS(localFile, 1024*10, fs.ProcessLocalBuffer, cfile)
	cfile.Close()
}
