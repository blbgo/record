package store

import (
	"github.com/blbgo/general"
)

// Config provides config values for store
type Config interface {
	// DataPath must return the path of the directory where the database is or will be created
	DataPath() string
}

type config struct {
	DataPathValue string
}

// NewConfig provides a Config
func NewConfig(c general.Config) (Config, error) {
	r := &config{}
	var err error

	r.DataPathValue, err = c.Value("Record", "DataPath")
	if err != nil {
		return nil, err
	}

	return r, nil
}

// DataPath method of record.Config, returns the path where database files are or should be
// created
func (r *config) DataPath() string {
	return r.DataPathValue
}

type configInMem struct{}

// NewConfigInMem provides a Config with an empty string DataPath causing an in memory database
func NewConfigInMem() Config {
	return configInMem{}
}

// DataPath method of record.Config, returns blank path so db will be in memory
func (r configInMem) DataPath() string {
	return ""
}
