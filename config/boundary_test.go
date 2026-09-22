package config_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cocosip/venue/config"
)

func TestConfigSupportsFluentConstruction(t *testing.T) {
	cfg := config.New().
		WithMetadataDirectory("metadata").
		WithQuotaDirectory("quota").
		WithAutoCreateTenants(true).
		WithVolumes(
			config.NewVolumeConfig().
				WithVolumeID("volume-1").
				WithMountPath("storage").
				WithVolumeType("LocalFileSystem").
				WithShardingDepth(2),
		)

	if cfg.MetadataDirectory != "metadata" || cfg.QuotaDirectory != "quota" {
		t.Fatalf("directories = %q, %q", cfg.MetadataDirectory, cfg.QuotaDirectory)
	}
	if !cfg.AutoCreateTenants {
		t.Fatal("AutoCreateTenants = false")
	}
	if len(cfg.Volumes) != 1 || cfg.Volumes[0].VolumeID != "volume-1" {
		t.Fatalf("volumes = %#v", cfg.Volumes)
	}
}

func TestConfigTypesExposeSourceBindingTags(t *testing.T) {
	assertBindingTags(t, reflect.TypeOf(config.Config{}), map[reflect.Type]bool{})
}

func TestBaseConfigPackageHasNoSourceAdapterImports(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	forbidden := map[string]bool{
		"encoding/json":          true,
		"github.com/spf13/viper": true,
		"gopkg.in/yaml.v3":       true,
	}

	entries, err := filepath.Glob(filepath.Join(root, "*.go"))
	if err != nil {
		t.Fatalf("glob config package: %v", err)
	}
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err == nil && forbidden[importPath] {
				t.Errorf("base config file %s imports source adapter %s", filepath.Base(path), importPath)
			}
		}
	}
}

func TestRuntimePackagesDoNotImportViper(t *testing.T) {
	configDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	repositoryRoot := filepath.Dir(configDirectory)

	err = filepath.WalkDir(repositoryRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".cache", ".kilo", "tmp":
				// Build output and scratch state are not runtime source; the Go
				// toolchain writes generated files there while this test walks.
				return filepath.SkipDir
			}
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			if relative == ".git" || relative == ".kilo" || relative == "viperconfig" || relative == "examples" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err == nil && importPath == "github.com/spf13/viper" {
				relative, _ := filepath.Rel(repositoryRoot, path)
				t.Errorf("runtime file %s imports Viper", filepath.ToSlash(relative))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
}

func assertBindingTags(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	if typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || seen[typ] {
		return
	}
	seen[typ] = true

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		for _, key := range []string{"json", "yaml", "mapstructure"} {
			value, ok := field.Tag.Lookup(key)
			if !ok || value == "" {
				t.Errorf("%s.%s is missing %s binding tag", typ.Name(), field.Name, key)
			}
			if field.Name == "Logging" && value != "-" {
				t.Errorf("Config.Logging %s tag = %q, want -", key, value)
			}
		}
		if field.Name != "Logging" {
			assertBindingTags(t, field.Type, seen)
		}
	}
}
