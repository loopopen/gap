package gapc

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/loopopen/gap/internal/pkgs/logx"
	"github.com/loopopen/gap/internal/pkgs/setx"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

//go:embed subscribe.tmpl
var tmplTxt string

type Generator struct {
	pkg             *packages.Package
	fset            *token.FileSet
	flags           *Flags
	tmp             *template.Template
	data            *TmplData
	isFuncSpecified bool
	fileNameMap     map[string]string
}

func NewGenerator() *Generator {
	return &Generator{
		fileNameMap: make(map[string]string),
	}
}

func (g *Generator) tmpl() *template.Template {
	if g.tmp == nil {
		g.tmp = template.Must(template.New("subscribe.tmpl").Parse(tmplTxt))
	}
	return g.tmp
}

func (g *Generator) ParseFlags() {
	// flag.Usage = func() {
	// 	log.Println("Usage: gapc [command] [options]")
	// }
	funcNames := flag.String("func", "", "comma-separated list of function names")
	filename := flag.String("file", "", "the targe go file to generate, typical value: $GOFILE")
	annotation := flag.String("anno", "@subscribe", "the annotation to mark the handler functions")
	group := flag.String("group", "", "the consumer group of the subscriber")
	separate := flag.Bool("separate", false, "each type has its own go file")
	sep := flag.Bool("sep", false, "each type has its own go file (alias for separate)")
	verbose := flag.Bool("verbose", false, "verbose output")
	v := flag.Bool("v", false, "verbose output (alias for verbose)")
	raw := flag.Bool("raw", false, "raw source")
	r := flag.Bool("r", false, "raw source (alias for raw)")
	flag.Parse()

	var funcs []string
	if *funcNames != "" {
		funcs = strings.Split(*funcNames, ",")
	}

	dir := "."

	if *filename != "" {
		if filepath.Ext(*filename) != ".go" {
			logx.Fatalf("file must be a go file: %s", *filename)
		}
		fp := filepath.Join(dir, *filename)
		_, err := os.Stat(fp)
		if err != nil && !os.IsExist(err) {
			logx.Fatalf("file not exists: %s", fp)
		}
	}

	anno := *annotation
	if !strings.HasPrefix(anno, at) {
		anno = at + anno
	}

	g.isFuncSpecified = *funcNames != "" && *funcNames != star
	sep_ := g.isFuncSpecified || *sep || *separate

	g.flags = &Flags{
		Dir:        dir,
		FuncNames:  funcs,
		FileName:   *filename,
		Annotation: anno,
		Group:      *group,
		Separate:   sep_,
		Verbose:    *verbose || *v,
		Raw:        *raw || *r,
	}
}

func (g *Generator) LoadPackage(patterns ...string) map[string]*packages.Package {
	patterns = append(patterns, dot)

	// if g.flags.Verbose {
	// 	var keys []string
	// 	for key := range g.overlay {
	// 		keys = append(keys, key)
	// 	}
	// 	logx.Debugf("load with patterns: %v, with overlay: %v", patterns, keys)
	// }

	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedSyntax |
			packages.NeedTypes |
			packages.NeedTypesInfo,
		Tests: false,
		Dir:   g.flags.Dir,
		// Overlay: g.overlay,
	}
	pkgs, err := loadPkgs(cfg, patterns...)
	if err != nil {
		logx.Fatalf("%s", err)
	}

	for _, pat := range patterns {
		if _, ok := pkgs[pat]; !ok {
			logx.Fatalf("no package found with pattern %s", pat)
		}
	}

	primaryPkg := pkgs[dot]
	// cf := g.commonFlags
	// if cf.FileName == "" && Contains(cf.TypeNames, star) {
	// 	//find the file in which cmdline exists
	// end:
	// 	for i, f := range primaryPkg.Syntax {
	// 		for _, cg := range f.Comments {
	// 			for _, c := range cg.List {
	// 				if !findCmdLine(c.Text, cf.CmdLine) {
	// 					continue
	// 				}
	// 				filename := filepath.Base(primaryPkg.GoFiles[i])
	// 				g.allInOneFile = filename
	// 				break end
	// 			}
	// 		}
	// 	}
	// }

	g.pkg = primaryPkg
	return pkgs
}

func loadPkgs(cfg *packages.Config, patterns ...string) (map[string]*packages.Package, error) {
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*packages.Package, len(patterns))
	for _, pat := range patterns {
		for _, pkg := range pkgs {
			if hasMultiPkgs(pkg) {
				logx.Fatalf("multiple packages found in %s", pkg.Dir)
			}

			if pkg.PkgPath == pat {
				result[pat] = pkg
				break
			}

			isPath := strings.HasPrefix(pat, ".") || strings.HasPrefix(pat, "/")
			if isPath {
				absPat, _ := filepath.Abs(filepath.Join(cfg.Dir, pat))
				if pkg.Dir == absPat {
					_, ok := result[pat]
					if ok {
						logx.Fatalf("multiple packages found in %s", pat)
					}
					result[pat] = pkg
				}
			}
		}
	}

	return result, nil
}

func hasMultiPkgs(pkg *packages.Package) bool {
	for _, e := range pkg.Errors {
		if strings.Contains(e.Msg, "found packages") {
			return true
		}
	}
	return false
}

func (g *Generator) Generate() map[string][]byte {
	g.confirmFuncs()

	srcMap := make(map[string][]byte)
	var srcList [][]byte
	for _, fnName := range g.flags.FuncNames {
		data := g.MakeData(fnName)
		if data == nil {
			continue
		}
		src := g.generateOne(data)
		if len(src) == 0 {
			continue
		}

		filename := g.fileName(fnName, false)

		if g.flags.Separate {
			srcMap[filename] = src
		} else {
			srcList = append(srcList, src)
		}
	}

	if len(srcList) > 0 {
		src, err := MergeSources(srcList...)
		if err != nil {
			logx.Fatalf("merge sources error: %s", err)
		}
		if len(src) > 0 {
			srcMap[g.fileName("", false)] = src
		}
	}

	return srcMap
}

func (g *Generator) generateOne(data any) []byte {
	v := g.flags.Verbose
	r := g.flags.Raw

	if v {
		logx.DebugJSON("template data:\n", data)
	}

	var buff bytes.Buffer
	err := g.tmpl().Execute(&buff, data)
	if err != nil {
		logx.Fatalf("failed to generate: %v", err)
	}
	src := buff.Bytes()

	if v {
		logx.Debug("raw source code:\n", string(src))
	}
	if r {
		return src
	}

	src, err = FormatSrc(src)
	if err != nil {
		logx.Fatalf("failed to format source: %v", err)
	}
	return src
}

func getGoFile(pkg *packages.Package, typeName string) string {
	for _, obj := range pkg.TypesInfo.Defs {
		if obj == nil {
			continue
		}

		_, ok := obj.(*types.TypeName)
		if !ok {
			continue
		}

		if obj.Name() == typeName {
			pos := pkg.Fset.Position(obj.Pos())
			return filepath.Base(pos.Filename)
		}
	}
	return ""
}

func (g *Generator) listTypes() []string {
	var funcNames []string
	for _, f := range g.pkg.Syntax {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Doc == nil {
				continue
			}

			_, _, ok = parseSubComment(g.flags.Annotation, fn.Doc.Text())
			if !ok {
				continue
			}

			funcNames = append(funcNames, fn.Name.Name)
		}
	}
	return funcNames
}

func (g *Generator) confirmFuncs() {
	funcNames := g.flags.FuncNames
	if g.isFuncSpecified {
		for _, fn := range funcNames {
			goFile := getGoFile(g.pkg, fn)
			if g.flags.FileName == "" {
				g.fileNameMap[fn] = goFile
			} else if g.flags.FileName != goFile {
				logx.Fatalf("function %s is not in the specified file", fn)
			}
		}
	} else {
		g.flags.FuncNames = g.listTypes()
	}
}

func (g *Generator) fileName(funcName string, pkgScope bool) string {
	cmd := "gapc"
	if pkgScope {
		return fmt.Sprintf("%s.%s.go", cmd, funcName)
	}
	fileName := g.flags.FileName
	// if fileName == "" {
	// 	fileName = g.allInOneFile
	// }
	// if fileName == "" {
	// 	fileName = g.fileNameMap[typeName]
	// }

	gofile := strings.TrimSuffix(fileName, ".go")
	if funcName == "" {
		return fmt.Sprintf("%s.%s.go", gofile, cmd)
	}
	if !ast.IsExported(funcName) {
		funcName = "_" + funcName
	}
	return fmt.Sprintf("%s.%s.%s.go", gofile, cmd, strings.ToLower(funcName))
}

func FormatSrc(src []byte) ([]byte, error) {
	// format imports
	src, err := imports.Process("_.go", src, nil)
	if err != nil {
		return nil, err
	}

	// format source code
	src, err = format.Source(src)
	if err != nil {
		fmt.Println(string(src))
		return nil, err
	}
	return src, nil
}

func MergeSources(sources ...[]byte) ([]byte, error) {
	if len(sources) == 0 {
		return nil, nil
	}

	fset := token.NewFileSet()
	var files []*ast.File

	for i, src := range sources {
		file, err := parser.ParseFile(fset, fmt.Sprintf("src%d.go", i), src, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parse src%d.go: %w", i, err)
		}
		files = append(files, file)
	}

	pkgName := files[0].Name.Name

	importSet := setx.MakeSet[string]()
	var importDecls []ast.Decl

	for _, f := range files {
		for _, imp := range f.Imports {
			key := imp.Path.Value
			if imp.Name != nil {
				key += imp.Name.Name
			}
			if !importSet.Has(key) {
				importSet.Adds(key)
				importDecls = append(importDecls, &ast.GenDecl{
					Tok:   token.IMPORT,
					Specs: []ast.Spec{imp},
				})
			}
		}
	}

	var buf bytes.Buffer

	// ---- header ----
	if len(files[0].Comments) > 0 {
		fmt.Fprint(&buf, "// ")
		fmt.Fprintln(&buf, files[0].Comments[0].Text())
		fmt.Fprintln(&buf)
	}

	// ---- package ----
	fmt.Fprintf(&buf, "package %s\n\n", pkgName)

	// ---- imports ----
	if len(importDecls) > 0 {
		for _, decl := range importDecls {
			printer.Fprint(&buf, fset, decl)
			fmt.Fprintln(&buf)
		}
		fmt.Fprintln(&buf)
	}

	// ---- decls ----
	for _, f := range files {
		for _, decl := range f.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
				continue
			}

			if err := printDeclWithOwnComments(&buf, fset, f, decl, pkgName); err != nil {
				return nil, fmt.Errorf("print decl: %w", err)
			}
			fmt.Fprintln(&buf)
		}
	}

	out, err := FormatSrc(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format merged source: %w", err)
	}
	return noopFix(out), nil
}

func printDeclWithOwnComments(buf *bytes.Buffer, fset *token.FileSet, file *ast.File, decl ast.Decl, pkgName string) error {
	var bf bytes.Buffer
	mf := &ast.File{
		Name:  file.Name,
		Decls: []ast.Decl{decl},
	}

	mf.Comments = append(mf.Comments, attachCommentsForDecl(file, decl)...)

	err := printer.Fprint(&bf, fset, mf)
	if err != nil {
		return err
	}

	reg := regexp.MustCompile("(?m)^package " + pkgName + "$")
	src := bf.Bytes()
	if !reg.Match(src) {
		logx.Fatal("cannot merge fiels with different packages")
	}
	src = reg.ReplaceAll(src, nil)
	buf.Write(src)
	return nil
}

func attachCommentsForDecl(file *ast.File, decl ast.Decl) []*ast.CommentGroup {
	var groups []*ast.CommentGroup
	declStart := decl.Pos()
	declEnd := decl.End()

	for _, cg := range file.Comments {
		if cg.Pos() >= declStart && cg.Pos() <= declEnd {
			groups = append(groups, cg)
			continue
		}

		if cg.End() <= declStart {
			if declStart-cg.End() < 10 {
				groups = append(groups, cg)
			}
		}
	}

	return groups
}

func noopFix(src []byte) []byte {
	noopFuncReg := regexp.MustCompile(`(\w+)\(\)\s*{\s*}`)
	src = noopFuncReg.ReplaceAll(src, []byte("$1() { /*noop*/ }"))
	return src
}

func (g *Generator) qualifier(pkg *types.Package) string {
	if pkg == nil {
		return ""
	}
	if pkg.Path() == g.pkg.PkgPath {
		return ""
	}
	return pkg.Name()
}
