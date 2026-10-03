package gapc

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"regexp"
	"strings"

	"github.com/loopopen/gap/internal/pkgs/logx"
)

func (g *Generator) CmdLine() string {
	return g.data.CmdLine
}

func (g *Generator) MakeData(funcName string) any {
	args := []string{gapc}
	args = append(args, os.Args[1:]...)
	g.data = &TmplData{
		CmdLine:     strings.Join(args, " "),
		PackageName: g.pkg.Name,
		FuncName:    funcName,
		PackagePath: g.pkg.PkgPath,
	}

	g.make(funcName)

	return g.data
}

func (g *Generator) make(funcName string) {
	anno := g.flags.Annotation
	for _, f := range g.pkg.Syntax {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}

			if fn.Name.Name != funcName {
				continue
			}

			var doc string
			if fn.Doc != nil {
				doc = fn.Doc.Text()
			}
			group, topic, ok := parseSubComment(anno, doc)
			if !ok {
				logx.Fatalf("function %s missing annotation %s", funcName, anno)
			}

			if group == "" {
				group = g.flags.Group
			}
			if group == "" {
				group = pkgPathAsGroup(g.pkg.PkgPath)
			}
			g.data.Group = group
			g.data.Topic = topic

			g.cookFunc(fn)
		}
	}
}

func pkgPathAsGroup(pkgPath string) string {
	if pkgPath == "" {
		return ""
	}
	xs := strings.Split(pkgPath, "/")
	if len(xs) == 1 {
		return xs[0]
	}
	if len(xs) == 2 || !strings.Contains(xs[0], ".") {
		return strings.Join(xs[:2], ".")
	}
	return strings.Join(xs[:3], ".")
}

func (g *Generator) cookFunc(fn *ast.FuncDecl) {
	var ds []Dependency
	var names []string

	if fn.Recv != nil {
		for _, r := range fn.Recv.List {
			ds = append(ds, Dependency{
				Name: "_" + r.Names[0].Name,
				Type: exprToString(r.Type),
			})
			g.data.HasRecv = true
		}
	}

	for _, p := range fn.Type.Params.List {
		ds = append(ds, Dependency{
			Name: p.Names[0].Name + "_",
			Type: exprToString(p.Type),
		})
		names = append(names, p.Names[0].Name+"_")
	}
	g.data.Dependencies = ds
	g.data.DependencyList = strings.Join(names, ", ")

	msgType, ok := g.checkHandlerType(fn)
	if !ok {
		logx.Fatalf("function %s has invalid signature", fn.Name.Name)
	}

	if g.data.Topic == "" {
		evtIface := makeEventIface()
		if !types.AssignableTo(msgType, evtIface) {
			logx.Fatalf("topic is undefined at function %s", fn.Name.Name)
		}
		ptr, ok := msgType.(*types.Pointer)
		if ok {
			typ := types.TypeString(ptr.Elem(), g.qualifier)
			g.data.TopicExpr = fmt.Sprintf("new(%s).Topic()", typ)
		} else {
			typ := types.TypeString(msgType, g.qualifier)
			g.data.TopicExpr = fmt.Sprintf("%s{}.Topic()", typ)
		}
	}

	g.data.MsgType = types.TypeString(msgType, g.qualifier)
}

func (g *Generator) checkHandlerType(fn *ast.FuncDecl) (types.Type, bool) {
	if len(fn.Type.Results.List) != 1 {
		logx.Fatalf("function %s has invalid signature", fn.Name.Name)
	}
	result := fn.Type.Results.List[0]
	handlerType := g.pkg.TypesInfo.TypeOf(result.Type)
	handler, ok := handlerType.(*types.Alias)
	if !ok {
		return nil, false
	}
	const (
		path = "github.com/loopopen/gap"
		name = "Handler"
	)
	if handler.Obj().Pkg().Path() != path || handler.Obj().Name() != name {
		return nil, false
	}
	args := handler.TypeArgs()
	if args == nil || args.Len() != 1 {
		return nil, false
	}
	return args.At(0), true
}

func parseSubComment(anno, doc string) (string, string, bool) {
	subReg := regexp.MustCompile(`(?im)^` + anno)
	if !subReg.MatchString(doc) {
		return "", "", false
	}
	var group, topic string
	groupReg := regexp.MustCompile(fmt.Sprintf("(?im)^%s:%s", anno, `.*?\Wgroup="([\w\.]+)"(;.*|\s*)$`))
	ms := groupReg.FindStringSubmatch(doc)
	if len(ms) > 1 {
		group = ms[1]
	}
	topicReg := regexp.MustCompile(fmt.Sprintf("(?im)^%s:%s", anno, `.*?\Wtopic="([\w\.]+)"(;.*|\s*)$`))
	ms = topicReg.FindStringSubmatch(doc)
	if len(ms) > 1 {
		topic = ms[1]
	}
	return group, topic, true
}

func exprToString(expr ast.Expr) string {
	var buf bytes.Buffer
	err := printer.Fprint(&buf, token.NewFileSet(), expr)
	if err != nil {
		logx.Fatalf("print expr: %s", err)
	}
	return buf.String()
}

// func parseMsgType(result string) string {
// 	typReg := regexp.MustCompile(`\w+\.Handler\[(.*)\]`)
// 	ms := typReg.FindStringSubmatch(result)
// 	if len(ms) > 1 {
// 		return ms[1]
// 	}
// 	return ""
// }

func makeEventIface() types.Type {
	iface := types.NewInterfaceType(
		[]*types.Func{
			types.NewFunc(
				token.NoPos,
				nil,
				"Topic",
				types.NewSignatureType(nil, nil, nil, nil, types.NewTuple(types.NewVar(0, nil, "", types.Typ[types.String])), false),
			),
		},
		nil,
	).Complete()
	return iface
}
