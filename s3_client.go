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

func uploadDirToS3(sourceDir, bucketName, objectName string, endpoint, accessKey, secretKey string, useSSL bool) error {
	// Создаем клиент MinIO
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	// Создаем временный архив
	tempFile, err := os.CreateTemp("", "backup-*.tar.gz")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Создаем gzip-архив и tar-запись
	gw := gzip.NewWriter(tempFile)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Архивируем каталог
	if err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}

		if len(path) <= len(sourceDir) {
			header.Name = "" // или обработка ошибки
		} else {
			header.Name = filepath.ToSlash(path[len(sourceDir)+1:])
		} // Убираем путь к корню

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

	// Закрываем writer'ы
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}

	// Смотрим размер архива
	fileInfo, _ := tempFile.Stat()
	size := fileInfo.Size()

	tempFile.Seek(0, io.SeekStart) // Сбрасываем указатель на начало файла

	// Загружаем архив в S3 bucket
	log.Printf("Uploading %s to S3 bucket %s as %s", tempFile.Name(), bucketName, objectName)
	_, err = client.PutObject(context.Background(), bucketName, objectName, tempFile, size, minio.PutObjectOptions{})
	return err
}
