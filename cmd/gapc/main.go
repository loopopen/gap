package main

import (
	"fmt"
	"log"
	"os"

	"github.com/loopopen/gap/internal/gapc"
	"github.com/loopopen/gap/internal/pkgs/logx"
)

func main() {
	log.SetFlags(0)

	g := gapc.NewGenerator()
	g.ParseFlags()

	g.LoadPackage()
	srcMap := g.Generate()
	var fileNames []string
	for fname, src := range srcMap {
		notedownSrc(fname, src)
		fileNames = append(fileNames, fname)
	}

	if len(srcMap) == 0 {
		logx.Warnf("nothing generated: [%s]", g.CmdLine())
		return
	}

	log.Printf("🎉 go generate successfully: [%s]\n", g.CmdLine())
	for _, fn := range fileNames {
		log.Printf("\t%s\n", fn)
	}
}

func notedownSrc(fileName string, src []byte) {
	// write to tmpfile first
	tmpFile, err := os.CreateTemp(".", fmt.Sprintf(".%s_", fileName))
	defer func() {
		if tmpFile != nil {
			_ = tmpFile.Close()
		}
	}()
	if err != nil {
		logx.Fatalf("creating temporary file for output: %s", err)
	}
	_, err = tmpFile.Write(src)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		logx.Fatalf("writing output: %s", err)
	}
	tmpFile.Close()

	// rename tmpfile to output file
	err = os.Rename(tmpFile.Name(), fileName)
	if err != nil {
		logx.Fatalf("moving tempfile to output file: %s", err)
	}
}
