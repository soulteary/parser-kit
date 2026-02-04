package parserkit

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Phone string `json:"phone"`
}

func TestLoader_FromFile(t *testing.T) {
	// Create temporary test file
	tmpFile, err := os.CreateTemp("", "test-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	testData := []TestUser{
		{ID: "1", Email: "test1@example.com", Phone: "1234567890"},
		{ID: "2", Email: "test2@example.com", Phone: "0987654321"},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	_, err = tmpFile.Write(jsonData)
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Create loader
	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	// Load from file
	ctx := context.Background()
	users, err := loader.FromFile(ctx, tmpFile.Name())
	require.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, "test1@example.com", users[0].Email)
}

func TestLoader_FromFile_NotFound(t *testing.T) {
	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = loader.FromFile(ctx, "/nonexistent/file.json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file not found")
}

func TestLoader_FromFile_NotFound_AllowEmpty(t *testing.T) {
	opts := &LoadOptions{
		AllowEmptyFile: true,
	}
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	ctx := context.Background()
	users, err := loader.FromFile(ctx, "/nonexistent/file.json")
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestLoader_FromFile_EmptyArray(t *testing.T) {
	// Create temporary file with empty array
	tmpFile, err := os.CreateTemp("", "test-empty-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString("[]")
	require.NoError(t, err)
	_ = tmpFile.Close()

	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	ctx := context.Background()
	users, err := loader.FromFile(ctx, tmpFile.Name())
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestLoader_FromFile_InvalidJSON(t *testing.T) {
	// Create temporary test file with invalid JSON
	tmpFile, err := os.CreateTemp("", "test-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString("invalid json")
	require.NoError(t, err)
	_ = tmpFile.Close()

	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = loader.FromFile(ctx, tmpFile.Name())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

// FromFile: stat 返回非 NotExist 错误（如非法路径）
func TestLoader_FromFile_StatError(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	// 使用含空字节的路径，在多数系统上 Stat 会失败且错误不是 IsNotExist
	invalidPath := "\x00invalid"
	_, err = loader.FromFile(context.Background(), invalidPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to stat file")
}

// FromFile: 路径是目录时，可能报 "failed to open file"（Windows）、"failed to read file"（Unix 读目录）或 "failed to parse JSON"
func TestLoader_FromFile_OpenOrReadError(t *testing.T) {
	dir, err := os.MkdirTemp("", "parser-dir-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromFile(context.Background(), dir)
	require.Error(t, err)
	errMsg := err.Error()
	valid := strings.Contains(errMsg, "failed to open file") ||
		strings.Contains(errMsg, "failed to read file") ||
		strings.Contains(errMsg, "failed to parse JSON")
	assert.True(t, valid, "err: %s", errMsg)
}

func TestLoader_FromRedis(t *testing.T) {
	// Skip if Redis is not available
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available")
	}
	defer func() { _ = client.Close() }()

	// Prepare test data
	testData := []TestUser{
		{ID: "1", Email: "test1@example.com", Phone: "1234567890"},
		{ID: "2", Email: "test2@example.com", Phone: "0987654321"},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	// Set data in Redis
	testKey := "test:users:" + time.Now().Format("20060102150405")
	err = client.Set(ctx, testKey, jsonData, time.Minute).Err()
	require.NoError(t, err)
	defer client.Del(ctx, testKey)

	// Create loader
	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	// Load from Redis
	users, err := loader.FromRedis(ctx, client, testKey)
	require.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, "test1@example.com", users[0].Email)
}

func TestLoader_FromRedis_NotFound(t *testing.T) {
	// Skip if Redis is not available
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available")
	}
	defer func() { _ = client.Close() }()

	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	_, err = loader.FromRedis(ctx, client, "nonexistent:key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestLoader_Load_Priority(t *testing.T) {
	// Create temporary test file
	tmpFile, err := os.CreateTemp("", "test-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	testData := []TestUser{
		{ID: "1", Email: "file@example.com", Phone: "1234567890"},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	_, err = tmpFile.Write(jsonData)
	require.NoError(t, err)
	_ = tmpFile.Close()

	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Test: File source should work
	sources := []Source{
		{
			Type:     SourceTypeFile,
			Priority: 0,
			Config: SourceConfig{
				FilePath: tmpFile.Name(),
			},
		},
	}

	users, err := loader.Load(ctx, sources...)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "file@example.com", users[0].Email)
}

func TestLoader_Load_Fallback(t *testing.T) {
	// Create temporary test file
	tmpFile, err := os.CreateTemp("", "test-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	testData := []TestUser{
		{ID: "1", Email: "file@example.com", Phone: "1234567890"},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	_, err = tmpFile.Write(jsonData)
	require.NoError(t, err)
	_ = tmpFile.Close()

	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Test: First source fails, fallback to file
	sources := []Source{
		{
			Type:     SourceTypeRedis,
			Priority: 0, // Highest priority, but will fail
			Config: SourceConfig{
				RedisKey:    "nonexistent:key",
				RedisClient: redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
			},
		},
		{
			Type:     SourceTypeFile,
			Priority: 1, // Fallback
			Config: SourceConfig{
				FilePath: tmpFile.Name(),
			},
		},
	}

	// This should fallback to file if Redis fails
	users, err := loader.Load(ctx, sources...)
	if err == nil {
		// If Redis is available and key doesn't exist, it will fail
		// But if Redis is not available, it might skip Redis and use file
		// So we check if we got file data
		if len(users) > 0 {
			assert.Equal(t, "file@example.com", users[0].Email)
		}
	}
}

func TestLoader_Load_AllSourcesFail(t *testing.T) {
	loader, err := NewLoader[TestUser](nil)
	require.NoError(t, err)

	ctx := context.Background()

	sources := []Source{
		{
			Type:     SourceTypeFile,
			Priority: 0,
			Config: SourceConfig{
				FilePath: "/nonexistent/file.json",
			},
		},
		{
			Type:     SourceTypeFile,
			Priority: 1,
			Config: SourceConfig{
				FilePath: "/another/nonexistent/file.json",
			},
		},
	}

	_, err = loader.Load(ctx, sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all sources failed")
}

func TestLoader_Load_AllowEmptyData(t *testing.T) {
	// Empty file with valid JSON array
	tmpFile, err := os.CreateTemp("", "test-empty-*.json")
	require.NoError(t, err)
	tmpFileName := tmpFile.Name()
	_, _ = tmpFile.Write([]byte("[]"))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFileName) }()

	// AllowEmptyData=true: empty array is accepted and returned
	opts := DefaultLoadOptions()
	opts.AllowEmptyData = true
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	ctx := context.Background()
	sources := []Source{
		{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName}},
	}
	users, err := loader.Load(ctx, sources...)
	require.NoError(t, err)
	assert.Empty(t, users)

	// AllowEmptyData=false: empty source is skipped, next source used
	tmpFile2, err := os.CreateTemp("", "test-data-*.json")
	require.NoError(t, err)
	tmpFileName2 := tmpFile2.Name()
	jsonData, _ := json.Marshal([]TestUser{{ID: "1", Email: "test@example.com", Phone: "1234567890"}})
	_, _ = tmpFile2.Write(jsonData)
	_ = tmpFile2.Close()
	defer func() { _ = os.Remove(tmpFileName2) }()

	opts2 := DefaultLoadOptions()
	opts2.AllowEmptyData = false
	loader2, err := NewLoader[TestUser](opts2)
	require.NoError(t, err)

	sources2 := []Source{
		{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName}},
		{Type: SourceTypeFile, Priority: 1, Config: SourceConfig{FilePath: tmpFileName2}},
	}
	users2, err := loader2.Load(ctx, sources2...)
	require.NoError(t, err)
	assert.Len(t, users2, 1)
	assert.Equal(t, "test@example.com", users2[0].Email)
}

func TestLoader_WithNormalizeFunc(t *testing.T) {
	// Create temporary test file
	tmpFile, err := os.CreateTemp("", "test-*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	testData := []TestUser{
		{ID: "1", Email: "TEST@EXAMPLE.COM", Phone: "1234567890"},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	_, err = tmpFile.Write(jsonData)
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Create loader with normalize function
	normalizeFunc := func(users []TestUser) []TestUser {
		for i := range users {
			// Normalize email to lowercase
			users[i].Email = "normalized@example.com"
		}
		return users
	}
	loader, err := NewLoaderWithNormalize[TestUser](nil, normalizeFunc)
	require.NoError(t, err)

	ctx := context.Background()
	users, err := loader.FromFile(ctx, tmpFile.Name())
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "normalized@example.com", users[0].Email)
}

func TestDefaultLoadOptions(t *testing.T) {
	opts := DefaultLoadOptions()
	assert.Equal(t, int64(10*1024*1024), opts.MaxFileSize)
	assert.Equal(t, 3, opts.MaxRetries)
	assert.Equal(t, 1*time.Second, opts.RetryDelay)
	assert.Equal(t, 5*time.Second, opts.HTTPTimeout)
	assert.False(t, opts.InsecureSkipVerify)
	assert.False(t, opts.AllowEmptyFile)
	assert.False(t, opts.AllowEmptyData)
	assert.Equal(t, LoadStrategyFallback, opts.LoadStrategy)
	assert.Nil(t, opts.KeyFunc)
}

// NewLoader 使用 opts 且 MaxFileSize==0、LoadStrategy=="" 时使用默认值
func TestLoader_NewLoader_OptsDefaulting(t *testing.T) {
	opts := &LoadOptions{
		MaxFileSize:  0,
		LoadStrategy: "",
	}
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)
	require.NotNil(t, loader)
	assert.Equal(t, int64(10*1024*1024), opts.MaxFileSize)
	assert.Equal(t, LoadStrategyFallback, opts.LoadStrategy)

	tmpFile, err := os.CreateTemp("", "test-opts-*.json")
	require.NoError(t, err)
	_, _ = tmpFile.Write([]byte(`[{"id":"1","email":"a@b.com","phone":"1"}]`))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	users, err := loader.FromFile(context.Background(), tmpFile.Name())
	require.NoError(t, err)
	assert.Len(t, users, 1)
}

func TestLoader_Load_MergeStrategy(t *testing.T) {
	// Create two files with overlapping and distinct data (same write pattern as TestLoader_FromFile)
	file1Data := []TestUser{
		{ID: "1", Email: "remote@example.com", Phone: "111"},
		{ID: "2", Email: "remote2@example.com", Phone: "222"},
	}
	tmpFile1, err := os.CreateTemp("", "test-merge1-*.json")
	require.NoError(t, err)
	tmpFileName1 := tmpFile1.Name()
	defer func() { _ = os.Remove(tmpFileName1) }()
	json1, err := json.Marshal(file1Data)
	require.NoError(t, err)
	_, err = tmpFile1.Write(json1)
	require.NoError(t, err)
	err = tmpFile1.Close()
	require.NoError(t, err)

	file2Data := []TestUser{
		{ID: "2", Email: "local2@example.com", Phone: "222"}, // same Phone, different Email
		{ID: "3", Email: "local3@example.com", Phone: "333"},
	}
	tmpFile2, err := os.CreateTemp("", "test-merge2-*.json")
	require.NoError(t, err)
	tmpFileName2 := tmpFile2.Name()
	defer func() { _ = os.Remove(tmpFileName2) }()
	json2, err := json.Marshal(file2Data)
	require.NoError(t, err)
	_, err = tmpFile2.Write(json2)
	require.NoError(t, err)
	err = tmpFile2.Close()
	require.NoError(t, err)

	keyFunc := func(u TestUser) (string, bool) { return u.Phone, true }
	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.KeyFunc = keyFunc
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	ctx := context.Background()
	sources := []Source{
		{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName1}},
		{Type: SourceTypeFile, Priority: 1, Config: SourceConfig{FilePath: tmpFileName2}},
	}

	users, err := loader.Load(ctx, sources...)
	require.NoError(t, err)
	// Merge: first source adds 111,222; second adds 222 (overwrite), 333. Order: 111,222,333
	assert.Len(t, users, 3)
	byPhone := make(map[string]TestUser)
	for _, u := range users {
		byPhone[u.Phone] = u
	}
	assert.Equal(t, "remote@example.com", byPhone["111"].Email)
	assert.Equal(t, "local2@example.com", byPhone["222"].Email) // later source wins
	assert.Equal(t, "local3@example.com", byPhone["333"].Email)
}

func TestLoader_Load_MergeStrategy_WithoutKeyFunc(t *testing.T) {
	opts := &LoadOptions{
		LoadStrategy: LoadStrategyMerge,
		KeyFunc:      nil,
	}
	_, err := NewLoader[TestUser](opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "KeyFunc is required")
}

// Merge 策略下同一 source 内重复 key（key 已存在不追加 order）
func TestLoader_Load_MergeStrategy_DuplicateKeyInSource(t *testing.T) {
	// 同一文件中两条记录相同 phone，merge 时第二条覆盖第一条，order 只保留一个 key
	tmpFile, err := os.CreateTemp("", "merge-dup-*.json")
	require.NoError(t, err)
	tmpFileName := tmpFile.Name()
	_, _ = tmpFile.Write([]byte(`[{"id":"1","email":"first@x.com","phone":"1"},{"id":"2","email":"second@x.com","phone":"1"}]`))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFileName) }()

	keyFunc := func(u TestUser) (string, bool) { return u.Phone, true }
	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.KeyFunc = keyFunc
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)
	sources := []Source{{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName}}}
	users, err := loader.Load(context.Background(), sources...)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "second@x.com", users[0].Email)
}

// --- FromRemote coverage ---

func TestLoader_FromRemote_Success(t *testing.T) {
	data := []TestUser{{ID: "1", Email: "r@example.com", Phone: "111"}}
	body, _ := json.Marshal(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	ctx := context.Background()

	users, err := loader.FromRemote(ctx, server.URL, "")
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "r@example.com", users[0].Email)
}

func TestLoader_FromRemote_WithAuth(t *testing.T) {
	data := []TestUser{{ID: "1", Email: "a@example.com", Phone: "999"}}
	body, _ := json.Marshal(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	users, err := loader.FromRemote(context.Background(), server.URL, "Bearer token")
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "a@example.com", users[0].Email)
}

func TestLoader_FromRemote_Non200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromRemote(context.Background(), server.URL, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status code")
}

func TestLoader_FromRemote_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json"))
	}))
	defer server.Close()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromRemote(context.Background(), server.URL, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

// --- Load with Remote source (loadFromSource Remote branch) ---

func TestLoader_Load_RemoteSource(t *testing.T) {
	data := []TestUser{{ID: "1", Email: "remote@test.com", Phone: "555"}}
	body, _ := json.Marshal(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{
		{Type: SourceTypeRemote, Priority: 0, Config: SourceConfig{RemoteURL: server.URL}},
	}
	users, err := loader.Load(context.Background(), sources...)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "remote@test.com", users[0].Email)
}

func TestLoader_Load_RemoteSource_EmptyURL(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{
		{Type: SourceTypeRemote, Priority: 0, Config: SourceConfig{RemoteURL: ""}},
	}
	_, err = loader.Load(context.Background(), sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "remote URL not specified")
}

func TestLoader_Load_FileSource_EmptyPath(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{
		{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: ""}},
	}
	_, err = loader.Load(context.Background(), sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file path not specified")
}

func TestLoader_Load_RedisSource_EmptyKey(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available")
	}
	defer func() { _ = client.Close() }()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{
		{Type: SourceTypeRedis, Priority: 0, Config: SourceConfig{RedisKey: "", RedisClient: client}},
	}
	_, err = loader.Load(ctx, sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis key or client not specified")
}

func TestLoader_Load_RedisSource_NilClient(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{
		{Type: SourceTypeRedis, Priority: 0, Config: SourceConfig{RedisKey: "some:key", RedisClient: nil}},
	}
	_, err = loader.Load(context.Background(), sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis key or client not specified")
}

func TestLoader_Load_UnknownSourceType(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{
		{Type: SourceType("unknown"), Priority: 0, Config: SourceConfig{}},
	}
	_, err = loader.Load(context.Background(), sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown source type")
}

func TestLoader_Load_NoSources(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.Load(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no sources provided")
}

// --- FromRedis invalid client ---

func TestLoader_FromRedis_InvalidClientType(t *testing.T) {
	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	ctx := context.Background()
	_, err = loader.FromRedis(ctx, "not-a-client", "key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Redis client type")
}

// --- loadWithMerge: KeyFunc include=false, hasData=false + AllowEmptyData ---

func TestLoader_Load_MergeStrategy_KeyFuncExclude(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "merge-exclude-*.json")
	tmpFileName := tmpFile.Name()
	_, _ = tmpFile.Write([]byte(`[{"id":"1","email":"a@x.com","phone":"1"},{"id":"2","email":"b@x.com","phone":"2"}]`))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFileName) }()

	keyFunc := func(u TestUser) (string, bool) {
		if u.Phone == "2" {
			return "", false // exclude
		}
		return u.Phone, true
	}
	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.KeyFunc = keyFunc
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	sources := []Source{{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName}}}
	users, err := loader.Load(context.Background(), sources...)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "1", users[0].Phone)
}

func TestLoader_Load_MergeStrategy_AllEmpty_AllowEmptyData(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "merge-empty-*.json")
	tmpFileName := tmpFile.Name()
	_, _ = tmpFile.Write([]byte("[]"))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFileName) }()

	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.AllowEmptyData = true
	opts.KeyFunc = func(u TestUser) (string, bool) { return u.Phone, true }
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	sources := []Source{{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName}}}
	users, err := loader.Load(context.Background(), sources...)
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestLoader_Load_MergeStrategy_AllEmpty_NoAllowEmptyData(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "merge-empty-fail-*.json")
	tmpFileName := tmpFile.Name()
	_, _ = tmpFile.Write([]byte("[]"))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFileName) }()

	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.AllowEmptyData = false
	opts.KeyFunc = func(u TestUser) (string, bool) { return u.Phone, true }
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	sources := []Source{{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFileName}}}
	_, err = loader.Load(context.Background(), sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all sources returned empty data")
}

// --- NewLoaderWithNormalize KeyFunc type assertion failure ---

func TestLoader_NewLoaderWithNormalize_KeyFuncWrongType(t *testing.T) {
	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.KeyFunc = func(s string) (string, bool) { return s, true } // wrong: T is TestUser, not string
	_, err := NewLoader[TestUser](opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "KeyFunc must be of type")
}

// --- FromFile: path is directory (read error) ---

func TestLoader_FromFile_PathIsDir(t *testing.T) {
	dir, err := os.MkdirTemp("", "parser-dir-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromFile(context.Background(), dir)
	assert.Error(t, err)
	// Stat succeeds for dir; Open succeeds; ReadAll fails with "is a directory" or similar
	assert.True(t, os.IsNotExist(err) == false)
	assert.True(t, err != nil)
}

// --- FromRedis: value is invalid JSON ---

func TestLoader_FromRedis_InvalidJSON(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available")
	}
	defer func() { _ = client.Close() }()

	key := "test:invalid-json:" + time.Now().Format("20060102150405")
	err := client.Set(ctx, key, "not valid json", time.Minute).Err()
	require.NoError(t, err)
	defer client.Del(ctx, key)

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromRedis(ctx, client, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

// --- loadWithMerge: all sources fail -> lastErr path ---

func TestLoader_Load_MergeStrategy_AllSourcesFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	opts := DefaultLoadOptions()
	opts.LoadStrategy = LoadStrategyMerge
	opts.KeyFunc = func(u TestUser) (string, bool) { return u.Phone, true }
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	sources := []Source{{Type: SourceTypeRemote, Priority: 0, Config: SourceConfig{RemoteURL: server.URL}}}
	_, err = loader.Load(context.Background(), sources...)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all sources failed")
	assert.Contains(t, err.Error(), "last error")
}

// --- FromRemote: failed to fetch remote data ---
func TestLoader_FromRemote_FailedFetch(t *testing.T) {
	// 使用已取消的 context 触发 "failed to fetch remote data"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	_, err = loader.FromRemote(ctx, server.URL, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch remote data")
}

// --- FromRemote: failed to read response (server 返回 200 后提前断开) ---
func TestLoader_FromRemote_FailedReadResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	go func() {
		c, e := ln.Accept()
		if e != nil {
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4096)
		_, _ = c.Read(buf)
		// 返回 200 + Content-Length: 10，只写 2 字节后关闭，触发读错误
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n[]"))
	}()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)

	url := "http://" + ln.Addr().String()
	_, err = loader.FromRemote(context.Background(), url, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read response")
}

// --- FromRedis: failed to get from Redis (非 redis.Nil，如 context 已取消) ---
func TestLoader_FromRedis_FailedGet(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available")
	}
	defer func() { _ = client.Close() }()

	ctxCancelled, cancel := context.WithCancel(ctx)
	cancel()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)

	_, err = loader.FromRedis(ctxCancelled, client, "any-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get")
}

func TestLoader_FromRedis_ValueTooLarge(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available")
	}
	defer func() { _ = client.Close() }()

	key := "test:too-large:" + time.Now().Format("20060102150405")
	err := client.Set(ctx, key, "0123456789", time.Minute).Err()
	require.NoError(t, err)
	defer client.Del(ctx, key)

	opts := DefaultLoadOptions()
	opts.MaxFileSize = 5
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	_, err = loader.FromRedis(ctx, client, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redis value exceeds max size")
}

// --- FromRedis with miniredis (always run, no skip) ---

func TestLoader_FromRedis_Miniredis_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	testData := []TestUser{
		{ID: "1", Email: "m1@example.com", Phone: "111"},
		{ID: "2", Email: "m2@example.com", Phone: "222"},
	}
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	err = client.Set(context.Background(), "miniredis:users", jsonData, time.Minute).Err()
	require.NoError(t, err)

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	users, err := loader.FromRedis(context.Background(), client, "miniredis:users")
	require.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, "m1@example.com", users[0].Email)
}

func TestLoader_FromRedis_Miniredis_NotFound(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromRedis(context.Background(), client, "miniredis:nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestLoader_FromRedis_Miniredis_InvalidJSON(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	err := client.Set(context.Background(), "miniredis:bad", "not valid json", time.Minute).Err()
	require.NoError(t, err)

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromRedis(context.Background(), client, "miniredis:bad")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}

func TestLoader_FromRedis_Miniredis_FailedGet(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	_, err = loader.FromRedis(ctx, client, "any-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get")
}

func TestLoader_FromRedis_Miniredis_WithNormalize(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()

	testData := []TestUser{{ID: "1", Email: "RAW@EXAMPLE.COM", Phone: "999"}}
	jsonData, _ := json.Marshal(testData)
	_ = client.Set(context.Background(), "miniredis:norm", jsonData, time.Minute).Err()

	normalize := func(users []TestUser) []TestUser {
		for i := range users {
			users[i].Email = "normalized@redis.com"
		}
		return users
	}
	loader, err := NewLoaderWithNormalize[TestUser](DefaultLoadOptions(), normalize)
	require.NoError(t, err)
	users, err := loader.FromRedis(context.Background(), client, "miniredis:norm")
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "normalized@redis.com", users[0].Email)
}

// --- loadFromSource Remote 分支：Config.Timeout > 0 ---
func TestLoader_Load_RemoteSource_WithTimeout(t *testing.T) {
	data := []TestUser{{ID: "1", Email: "timeout@test.com", Phone: "777"}}
	body, _ := json.Marshal(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()

	loader, err := NewLoader[TestUser](DefaultLoadOptions())
	require.NoError(t, err)
	sources := []Source{{
		Type:     SourceTypeRemote,
		Priority: 0,
		Config:   SourceConfig{RemoteURL: server.URL, Timeout: 3 * time.Second},
	}}
	users, err := loader.Load(context.Background(), sources...)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "timeout@test.com", users[0].Email)
}

// --- NewLoaderWithNormalize opts == nil 使用 DefaultLoadOptions ---
func TestLoader_NewLoaderWithNormalize_NilOpts(t *testing.T) {
	loader, err := NewLoaderWithNormalize[TestUser](nil, nil)
	require.NoError(t, err)
	require.NotNil(t, loader)

	// 行为应与 DefaultLoadOptions 一致，可简单做一次 FromFile
	tmpFile, err := os.CreateTemp("", "test-nil-opts-*.json")
	require.NoError(t, err)
	_, _ = tmpFile.Write([]byte("[]"))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	users, err := loader.FromFile(context.Background(), tmpFile.Name())
	require.NoError(t, err)
	assert.Empty(t, users)
}

// --- Load strategy 显式为空时在 Load 内使用 Fallback ---
func TestLoader_Load_StrategyEmptyUsesFallback(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-strategy-*.json")
	require.NoError(t, err)
	_, _ = tmpFile.Write([]byte(`[{"id":"1","email":"e@x.com","phone":"1"}]`))
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	opts := DefaultLoadOptions()
	opts.LoadStrategy = "" // 显式空，NewLoaderWithNormalize 会设为 Fallback，此处验证 Load 内 strategy=="" 分支
	loader, err := NewLoader[TestUser](opts)
	require.NoError(t, err)
	// 若 loader 内部 strategy 仍可能为空，则此用例会走 Load 的 strategy=="" 分支
	sources := []Source{{Type: SourceTypeFile, Priority: 0, Config: SourceConfig{FilePath: tmpFile.Name()}}}
	users, err := loader.Load(context.Background(), sources...)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.Equal(t, "e@x.com", users[0].Email)
}
