package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type WebSite struct {
	Name string `yaml:"name"`
	Path string `yaml:"path"`
}
type Config struct {
	Global struct {
		Host      string `yaml:"host"`
		Port      string `yaml:"port"`
		SourceDir string `yaml:"sourceDir"`
		DestDir   string `yaml:"destDir"`
		TmpDir    string `yaml:"tmpDir"`
		Archive   bool   `yaml:"archive"`
	} `yaml:"global"`
	Web_site struct {
		Enable bool      `yaml:"enable"`
		List   []WebSite `yaml:"list"`
	} `yaml:"web_site"`
	S3_config struct {
		Enable    bool   `yaml:"enable"`
		Endpoint  string `yaml:"endpoint"`
		AccessKey string `yaml:"accessKey"`
		SecretKey string `yaml:"secretKey"`
		UseSSL    bool   `yaml:"useSSL"`
		Bucket    string `yaml:"bucket"`
	} `yaml:"s3_config"`
	Database struct {
		Enable        bool     `yaml:"enable"`
		User          string   `yaml:"user"`
		Password      string   `yaml:"password"`
		Host          string   `yaml:"host"`
		Port          int      `yaml:"port"`
		Database_list []string `yaml:"database_list"`
	} `yaml:"database"`
}

func main() {
	var path string
	flag.StringVar(&path, "f", "config.yaml", "path to config file")
	flag.Parse()
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error reading yaml config: %v", err)
	}
	var cfg Config
	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}
	if cfg.Database.Enable {
		fmt.Println("Database backup enable")
		fmt.Println("Databases list:")
		for _, database := range cfg.Database.Database_list {
			fmt.Println(database)
		}
	} else {
		fmt.Println("Database backup disable")
	}
	if cfg.Web_site.Enable {
		fmt.Println("Web site backup enable")
		fmt.Println("Websites list:")
		for _, website := range cfg.Web_site.List {
			fmt.Printf("  Name: %s, Path: %s\n", website.Name, website.Path)
			objName := website.Name + "/" + website.Name + "_archive.tar.gz"
			err := uploadDirToS3(website.Path, cfg.Global.TmpDir, cfg.S3_config.Bucket, objName, cfg.S3_config.Endpoint, cfg.S3_config.AccessKey, cfg.S3_config.SecretKey, cfg.S3_config.UseSSL)
			if err != nil {
				log.Fatalf("Error uploading website to S3: %v", err)
			}
		}
	} else {
		fmt.Println("Web site backup disable")
	}
}
