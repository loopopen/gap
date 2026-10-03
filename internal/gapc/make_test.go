package gapc

import (
	"testing"
)

func Test_parseSubComment(t *testing.T) {
	type args struct {
		anno string
		doc  string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
		want2 bool
	}{
		{
			name: "normal case",
			args: args{
				anno: "@subscribe",
				doc:  `@subscribe: group="grp1";topic="tp1"`,
			},
			want:  "grp1",
			want1: "tp1",
			want2: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := parseSubComment(tt.args.anno, tt.args.doc)
			if got != tt.want {
				t.Errorf("parseSubComment() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseSubComment() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("parseSubComment() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_pkgPathAsGroup(t *testing.T) {
	type args struct {
		pkgPath string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty",
			args: args{
				pkgPath: "",
			},
			want: "",
		},
		{
			name: "non-url",
			args: args{
				pkgPath: "examples/proj/a/b",
			},
			want: "examples.proj",
		},
		{
			name: "with url",
			args: args{
				pkgPath: "github.com/loopopen/proj/a/b",
			},
			want: "github.com.loopopen.proj",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pkgPathAsGroup(tt.args.pkgPath); got != tt.want {
				t.Errorf("pkgPathAsGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}
