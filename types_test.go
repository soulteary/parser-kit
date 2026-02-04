package parserkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceTypeConstants(t *testing.T) {
	assert.Equal(t, SourceType("file"), SourceTypeFile)
	assert.Equal(t, SourceType("redis"), SourceTypeRedis)
	assert.Equal(t, SourceType("remote"), SourceTypeRemote)
}

func TestLoadStrategyConstants(t *testing.T) {
	assert.Equal(t, LoadStrategy("fallback"), LoadStrategyFallback)
	assert.Equal(t, LoadStrategy("merge"), LoadStrategyMerge)
}

func TestDefaultLoadOptions_NonZero(t *testing.T) {
	opts := DefaultLoadOptions()
	require.NotNil(t, opts)
	assert.Greater(t, opts.MaxFileSize, int64(0), "MaxFileSize")
	assert.Greater(t, opts.MaxRetries, 0, "MaxRetries")
	assert.Greater(t, opts.RetryDelay, time.Duration(0), "RetryDelay")
	assert.Greater(t, opts.HTTPTimeout, time.Duration(0), "HTTPTimeout")
}
