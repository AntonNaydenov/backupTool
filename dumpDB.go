package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/AntonNaydenov/go-mysqldump"
	_ "github.com/go-sql-driver/mysql"
)

// Подключение к базе данных
func connectDB(user, password, host string, port int, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Бэкап схемы конкретной таблицы
func backupTableSchema(db *sql.DB, tableName, outputFile string) error {
	// Создаем директорию для дампа
	dumpDir := filepath.Dir(outputFile)
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return err
	}

	// Создаем уникальную временную поддиректорию для этой операции
	tmpDir := filepath.Join(dumpDir, fmt.Sprintf("tmp_%s_schema_%d", tableName, os.Getpid()))
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}
	// Удаляем временную директорию после завершения
	defer os.RemoveAll(tmpDir)

	// Регистрируем дампер
	dumper, err := mysqldump.Register(db, tmpDir, "20060102_150405")
	if err != nil {
		return err
	}
	defer dumper.Close()

	// Настраиваем конфигурацию для дампа только схемы с разделением по таблицам
	config := mysqldump.DumpConfig{
		IncludeSchema:  true,
		NoData:         true,
		Tables:         []string{tableName},
		SeparateTables: true, // Создаем отдельные файлы для каждой таблицы
		AddDropTable:   true, // Добавляем команду DROP TABLE перед CREATE TABLE
	}

	// Выполняем дамп
	result, err := dumper.Dump(config)
	if err != nil {
		return err
	}

	// Если дамп выполнен успешно, копируем файл в нужное место
	if len(result.Paths) > 0 {
		// Открываем исходный файл
		src, err := os.Open(result.Paths[0])
		if err != nil {
			return err
		}
		defer src.Close()

		// Создаем целевой файл
		dst, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		defer dst.Close()

		// Копируем содержимое
		_, err = io.Copy(dst, src)
		if err != nil {
			return err
		}
	}

	return nil
}

// Бэкап данных конкретной таблицы
func backupTableData(db *sql.DB, tableName, outputFile string) error {
	// Создаем директорию для дампа
	dumpDir := filepath.Dir(outputFile)
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		return err
	}

	// Создаем уникальную временную поддиректорию для этой операции
	tmpDir := filepath.Join(dumpDir, fmt.Sprintf("tmp_%s_data_%d", tableName, os.Getpid()))
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}
	// Удаляем временную директорию после завершения
	defer os.RemoveAll(tmpDir)

	// Регистрируем дампер
	dumper, err := mysqldump.Register(db, tmpDir, "20060102_150405")
	if err != nil {
		return err
	}
	defer dumper.Close()

	// Настраиваем конфигурацию для дампа только данных с разделением по таблицам
	config := mysqldump.DumpConfig{
		IncludeSchema:          false,
		NoData:                 false,
		Tables:                 []string{tableName},
		SeparateTables:         true, // Создаем отдельные файлы для каждой таблицы
		SingleTransaction:      true, // Используем одну транзакцию для дампа
		DisableForeignKeyCheck: true, // Отключаем проверку внешних ключей
	}

	// Выполняем дамп
	result, err := dumper.Dump(config)
	if err != nil {
		return err
	}

	// Если дамп выполнен успешно, копируем файл в нужное место
	if len(result.Paths) > 0 {
		// Открываем исходный файл
		src, err := os.Open(result.Paths[0])
		if err != nil {
			return err
		}
		defer src.Close()

		// Создаем целевой файл
		dst, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		defer dst.Close()

		// Копируем содержимое
		_, err = io.Copy(dst, src)
		if err != nil {
			return err
		}
	}

	return nil
}

// Получение списка таблиц
func getTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

// Дамп всей базы данных
func dumpDatabase(db *sql.DB, databaseName, outputDir string, concurrency int) error {
	// Создаем директорию для дампа, если она не существует
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	// Получаем список таблиц
	tables, err := getTables(db)
	if err != nil {
		return err
	}

	// Создаем каналы для управления параллелизмом
	type backupTask struct {
		tableName string
		isSchema  bool
		filePath  string
	}

	tasks := make(chan backupTask, len(tables)*2)
	errors := make(chan error, len(tables)*2)
	workers := make(chan struct{}, concurrency)

	// Запускаем горутины для обработки задач
	go func() {
		for _, tableName := range tables {
			tasks <- backupTask{tableName: tableName, isSchema: true, filePath: filepath.Join(outputDir, fmt.Sprintf("%s_%s_schema.sql", databaseName, tableName))}
			tasks <- backupTask{tableName: tableName, isSchema: false, filePath: filepath.Join(outputDir, fmt.Sprintf("%s_%s_data.sql", databaseName, tableName))}
		}
		close(tasks)
	}()

	// Обрабатываем задачи в нескольких горутинах
	for i := 0; i < concurrency; i++ {
		go func() {
			workers <- struct{}{}        // Занимаем слот воркера
			defer func() { <-workers }() // Освобождаем слот воркера

			for task := range tasks {
				var err error
				if task.isSchema {
					err = backupTableSchema(db, task.tableName, task.filePath)
				} else {
					err = backupTableData(db, task.tableName, task.filePath)
				}
				if err != nil {
					errors <- fmt.Errorf("failed to backup %s for table %s: %w", map[bool]string{true: "schema", false: "data"}[task.isSchema], task.tableName, err)
				}
			}
		}()
	}

	// Ждем завершения всех задач
	for i := 0; i < concurrency; i++ {
		workers <- struct{}{} // Ждем, пока все воркеры освободятся
	}

	// Проверяем наличие ошибок
	if len(errors) > 0 {
		// Возвращаем первую ошибку
		return <-errors
	}

	return nil
}
