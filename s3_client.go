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

func uploadDirToS3(sourceDir, tmpDir string, bucketName, objectName string, endpoint, accessKey, secretKey string, useSSL bool) error {
	// Создаем клиент MinIO
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	// Создаем временный архив
	// println("tmpDir:", tmpDir)
	// println("sourceDir:", sourceDir)
	// println("objectName:", objectName)
	// println("bucketName:", bucketName)
	// println("endpoint:", endpoint)
	// println("accessKey:", accessKey)
	// println("secretKey:", secretKey)
	// println("useSSL:", useSSL)
	// println("uploadDirToS3:", "uploadDirToS3")
	tempFile, err := os.CreateTemp(tmpDir, objectName)
	if err != nil {
		return err
	}
	println("tempFile.Name:", tempFile.Name())
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Создаем gzip-архив и tar-запись
	gw := gzip.NewWriter(tempFile)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Архивируем каталог
	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Ошибка доступа к файлу %s: %v", path, err)
			return nil // Пропустить проблемный файл
		}

		// Проверка прав на чтение файла
		if !info.Mode().IsRegular() || info.Mode()&0444 == 0 {
			log.Printf("Недостаточно прав для чтения файла %s", path)
			return nil
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			log.Printf("Ошибка создания заголовка для %s: %v", path, err)
			return nil
		}

		if len(path) <= len(sourceDir) {
			header.Name = "" // или обработка ошибки
		} else {
			header.Name = filepath.ToSlash(path[len(sourceDir)+1:])
		}

		if err := tw.WriteHeader(header); err != nil {
			log.Printf("Ошибка записи заголовка для %s: %v", path, err)
			return nil
		}

		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				log.Printf("Ошибка открытия файла %s: %v", path, err)
				return nil
			}
			defer file.Close()

			if _, err := io.Copy(tw, file); err != nil {
				log.Printf("Ошибка копирования данных из %s: %v", path, err)
				return nil
			}
		}
		return nil
	})

	if err != nil {
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
