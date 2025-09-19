package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func uploadDirToS3(
	sourceDir, tmpDir string,
	bucketName, objectPrefix string,
	endpoint, accessKey, secretKey string,
	useSSL, archive bool,
	concurrency int, // Новое: количество параллельных потоков
) error {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	if archive {
		// Режим архивации: один файл, один поток
		tempFile, err := os.CreateTemp(tmpDir, "backup-*.tar.gz")
		if err != nil {
			return err
		}
		defer os.Remove(tempFile.Name())
		defer tempFile.Close()

		gw := gzip.NewWriter(tempFile)
		defer gw.Close()
		tw := tar.NewWriter(gw)
		defer tw.Close()

		if err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}

			if len(path) <= len(sourceDir) {
				header.Name = ""
			} else {
				header.Name = filepath.ToSlash(path[len(sourceDir)+1:])
			}

			if err := tw.WriteHeader(header); err != nil {
				return err
			}

			if !info.IsDir() {
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				defer file.Close()

				if _, err := io.Copy(tw, file); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("failed to archive directory: %w", err)
		}

		if err := tw.Close(); err != nil {
			return err
		}
		if err := gw.Close(); err != nil {
			return err
		}

		fileInfo, _ := tempFile.Stat()
		size := fileInfo.Size()
		tempFile.Seek(0, io.SeekStart)

		log.Printf("Uploading archive %s to S3 bucket %s as %s", tempFile.Name(), bucketName, objectPrefix+"_archive.tar.gz")
		_, err = client.PutObject(context.Background(), bucketName, objectPrefix+"_archive.tar.gz", tempFile, size, minio.PutObjectOptions{})
		return err
	} else {
		// Режим без архивации: загрузка каждого файла отдельно, с параллелизмом
		var wg sync.WaitGroup
		files := make(chan string, concurrency)
		errorsChan := make(chan error, 100)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for path := range files {
					objectKey := objectPrefix + "/" + path[len(sourceDir)+1:]

					log.Printf("Uploading file %s to S3 bucket %s as %s", path, bucketName, objectKey)

					file, err := os.Open(path)
					if err != nil {
						errorsChan <- fmt.Errorf("failed to open file %s: %w", path, err)
						continue
					}
					defer file.Close()

					fileInfo, _ := file.Stat()
					size := fileInfo.Size()

					_, err = client.PutObject(context.Background(), bucketName, objectKey, file, size, minio.PutObjectOptions{})
					if err != nil {
						errorsChan <- fmt.Errorf("failed to upload %s: %w", path, err)
					}
				}
			}()
		}

		// Сбор всех файлов для загрузки
		if err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				files <- path
			}
			return nil
		}); err != nil {
			return fmt.Errorf("failed to collect files: %w", err)
		}

		close(files)
		wg.Wait()

		// Проверка ошибок
		if len(errorsChan) > 0 {
			var firstError error
			for err := range errorsChan {
				firstError = err
				log.Printf("Upload error: %v", err)
			}
			return firstError
		}

		return nil
	}
}
