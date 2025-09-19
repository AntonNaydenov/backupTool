package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func uploadDirToS3(
	sourceDir, tmpDir string,
	bucketName, objectName string,
	endpoint, accessKey, secretKey string,
	useSSL, archive bool) error {

	// Создаем клиент MinIO
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	if archive {
		// Режим архивации: создаём .tar.gz и загружаем его как один объект
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

		log.Printf("Uploading archive %s to S3 bucket %s as %s", tempFile.Name(), bucketName, objectName)
		_, err = client.PutObject(context.Background(), bucketName, objectName, tempFile, size, minio.PutObjectOptions{})
		return err
	} else {
		// Режим без архивации: загружаем каждый файл отдельно
		return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil // Пропускаем директории
			}

			// Формируем имя объекта в S3 (удаляем префикс sourceDir)
			objectKey := objectName[:len(objectName)-len("_archive.tar.gz")] + "/" +
				path[len(sourceDir)+1:]

			log.Printf("Uploading file %s to S3 bucket %s as %s", path, bucketName, objectKey)

			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			fileInfo, _ := file.Stat()
			size := fileInfo.Size()

			_, err = client.PutObject(context.Background(), bucketName, objectKey, file, size, minio.PutObjectOptions{})
			return err
		})
	}
}
