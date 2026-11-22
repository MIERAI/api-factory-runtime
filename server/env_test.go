package server

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Env 是项目 README 的来源(G22): 运行时的源码里读的每个环境变量都要在 Env 里,Env 里的每个都要真的被读。
func TestEnvListMatchesTheSource(t *testing.T) {
	read := regexp.MustCompile(`(?:Getenv|LookupEnv|envOr)\("([A-Z0-9_]+)"`)
	constant := regexp.MustCompile(`(?m)^const [A-Za-z]+Env\s*=\s*"([A-Z0-9_]+)"`)
	found := map[string]string{}
	err := filepath.Walk("..", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && (info.Name() == "internal" || info.Name() == "cmd" || info.Name() == ".git") {
			return filepath.SkipDir // 测试的工具 / 运维的命令行不是服务读的
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, re := range []*regexp.Regexp{read, constant} {
			for _, m := range re.FindAllStringSubmatch(string(b), -1) {
				found[m[1]] = path
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, e := range Env {
		if listed[e.Name] {
			t.Errorf("%s listed twice", e.Name)
		}
		listed[e.Name] = true
		switch e.When {
		case EnvRequired, EnvMessaging, EnvOptional, EnvTest:
		default:
			t.Errorf("%s: When %q", e.Name, e.When)
		}
		if _, ok := found[e.Name]; !ok {
			t.Errorf("%s is listed in Env but nothing reads it", e.Name)
		}
	}
	for name, where := range found {
		if !listed[name] {
			t.Errorf("%s is read (%s) but not listed in server.Env", name, where)
		}
	}
}
